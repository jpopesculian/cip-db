use ansi_term::{ANSIGenericString, Style};
use chrono::{prelude::*, DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use directories::ProjectDirs;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use reqwest::Url;
use serde::Deserialize;
use soup::prelude::*;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;

lazy_static::lazy_static! {
    static ref ROOT_URL: Url = Url::parse("https://www.cip-paris.fr").unwrap();
    static ref FILMS_URL: Url = ROOT_URL.join("/json/movies").unwrap();
    static ref CINEMAS_URL: Url = ROOT_URL.join("/json/cinemas").unwrap();
    static ref PROG_BAR_STYLE: ProgressStyle =
                ProgressStyle::with_template("  {msg:26} {bar:40}   {pos}/{len}")
                    .unwrap();
    static ref PARIS_OFFSET: FixedOffset = chrono::FixedOffset::east_opt(2 * 3600).unwrap();
    static ref NOW: DateTime<FixedOffset> = Utc::now().with_timezone(&*PARIS_OFFSET);
    static ref PROJECT_DIRS: ProjectDirs = ProjectDirs::from("com.github", "jpopesculian", "cip").unwrap();
    static ref DEFAULT_DB_PATH: PathBuf = PROJECT_DIRS.data_dir().join("data.db");
    static ref DAY_START: NaiveTime = NaiveTime::from_hms_opt(4, 0, 0).unwrap();
}

#[derive(Deserialize, Debug)]
struct Cinema {
    #[serde(default)]
    id: u64,
    #[serde(rename = "value")]
    name: String,
    #[serde(rename = "url")]
    url_path: String,
    address: String,
    #[serde(rename = "image1")]
    image_path: String,
}

impl Cinema {
    fn description(&self) -> String {
        format!("{} ({})", self.name, self.zip())
    }
    fn zip(&self) -> String {
        self.address.rsplit(' ').nth(1).unwrap().to_string()
    }
    fn url(&self) -> Url {
        ROOT_URL.join(&self.url_path).unwrap()
    }
    // fn image(&self) -> Url {
    //     ROOT_URL.join(&self.image_path).unwrap()
    // }
}

#[derive(Deserialize, Debug)]
struct Film {
    id: u64,
    #[serde(rename = "value")]
    name: String,
    #[serde(rename = "url")]
    url_path: String,
    image_path: String,
    #[serde(deserialize_with = "deserialize_null_default")]
    director: String,
    #[serde(rename = "releaseDate")]
    release_date: String,
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: serde::de::Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

impl Film {
    fn description(&self) -> String {
        format!("{} ({})", self.name, self.release_date)
    }
    fn url(&self) -> Url {
        ROOT_URL.join(&self.url_path).unwrap()
    }
    // fn image(&self) -> Url {
    //     ROOT_URL.join(&self.image_path).unwrap()
    // }
}

#[derive(Debug)]
struct Seance {
    id: u64,
    cinema_id: u64,
    film_id: u64,
    datetime: DateTime<FixedOffset>,
    version: String,
    url: Option<String>,
}

#[derive(Copy, Clone, Debug)]
enum Version {
    Original,
    French,
}

impl Version {
    fn short(&self) -> &'static str {
        match self {
            Self::Original => "VO",
            Self::French => "VF",
        }
    }
}

#[derive(Debug)]
struct QueryOptions {
    day: Option<NaiveDate>,
    time: Option<NaiveTime>,
    version: Option<Version>,
}

impl QueryOptions {
    fn after(&self) -> Option<DateTime<FixedOffset>> {
        if self.day.is_none() && self.time.is_none() {
            return None;
        }
        let start = self.day.unwrap_or_else(|| NOW.date_naive());
        let time = self.time.unwrap_or(*DAY_START);
        NaiveDateTime::new(start, time)
            .and_local_timezone(*PARIS_OFFSET)
            .earliest()
    }
    fn before(&self) -> Option<DateTime<FixedOffset>> {
        let day = (self.after()? + chrono::Duration::hours(24)).date_naive();
        NaiveDateTime::new(day, *DAY_START)
            .and_local_timezone(*PARIS_OFFSET)
            .earliest()
    }
}

#[derive(Debug)]
struct QueryResult {
    cinema: Cinema,
    film: Film,
    seance: Seance,
}

pub struct Database(Arc<Pool<SqliteConnectionManager>>);

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Self {
        std::fs::create_dir_all(path.as_ref().parent().unwrap()).unwrap();
        let manager = SqliteConnectionManager::file(path);
        let pool = Pool::new(manager).unwrap();
        Self(Arc::new(pool))
    }

    pub fn delete(path: impl AsRef<Path>) {
        let path = path.as_ref();
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }

    pub fn conn(&self) -> Result<Connection, r2d2::Error> {
        self.0.get().map(Connection)
    }
}

pub struct Connection(PooledConnection<SqliteConnectionManager>);

impl std::ops::Deref for Connection {
    type Target = rusqlite::Connection;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Connection {
    fn create_cinemas(&self) -> rusqlite::Result<usize> {
        self.execute(
            "CREATE TABLE cinema (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                url_path TEXT NOT NULL,
                address TEXT NOT NULL,
                image_path TEXT NOT NULL
            )",
            (),
        )
    }

    fn insert_cinema(&self, cinema: &Cinema) -> rusqlite::Result<usize> {
        let mut statement = self.prepare_cached(
            "INSERT INTO cinema
                (id, name, url_path, address, image_path)
                VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        statement.execute(rusqlite::params![
            cinema.id,
            &cinema.name,
            &cinema.url_path,
            &cinema.address,
            &cinema.image_path,
        ])
    }

    fn create_films(&self) -> rusqlite::Result<usize> {
        self.execute(
            "CREATE TABLE film (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                url_path TEXT NOT NULL,
                image_path TEXT NOT NULL,
                director TEXT NOT NULL,
                release_date TEXT NOT NULL
            )",
            (),
        )
    }

    fn insert_film(&self, film: &Film) -> rusqlite::Result<usize> {
        let mut statement = self.prepare_cached(
            "INSERT INTO film
                (id, name, url_path, image_path, director, release_date)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;

        statement.execute(rusqlite::params![
            film.id,
            &film.name,
            &film.url_path,
            &film.image_path,
            &film.director,
            &film.release_date,
        ])
    }

    fn create_seances(&self) -> rusqlite::Result<usize> {
        self.execute(
            "CREATE TABLE seance (
                id INTEGER PRIMARY KEY NOT NULL,
                cinema_id INTEGER NOT NULL,
                film_id INTEGER NOT NULL,
                datetime TEXT NOT NULL,
                version TEXT NOT NULL,
                url TEXT,
                FOREIGN KEY(cinema_id) REFERENCES cinema(id),
                FOREIGN KEY(film_id) REFERENCES film(id)
            )",
            (),
        )
    }

    fn insert_seance(&self, seance: &Seance) -> rusqlite::Result<usize> {
        let mut statement = self.prepare_cached(
            "INSERT INTO seance
                (id, cinema_id, film_id, datetime, version, url)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;
        statement.execute(rusqlite::params![
            seance.id,
            seance.cinema_id,
            seance.film_id,
            seance.datetime.to_rfc3339(),
            &seance.version,
            &seance.url,
        ])
    }

    fn query_seances(&self, options: QueryOptions) -> rusqlite::Result<Vec<QueryResult>> {
        let mut where_clauses = Vec::new();
        if let Some(after) = options.after() {
            where_clauses.push(format!("datetime >= '{}'", after.to_rfc3339()));
        }
        if let Some(before) = options.before() {
            where_clauses.push(format!("datetime <= '{}'", before.to_rfc3339()));
        }
        if let Some(version) = options.version {
            where_clauses.push(format!("version = '{}'", version.short()));
        }
        let where_clause = if !where_clauses.is_empty() {
            format!("WHERE {}", where_clauses.join(" AND "))
        } else {
            String::new()
        };
        let mut stmt = self.prepare(&format!(
            "SELECT
                seance.id, cinema_id, film_id, datetime, version, url,
                cinema.name, cinema.url_path, cinema.address, cinema.image_path,
                film.name, film.url_path, film.image_path, film.director, film.release_date
            FROM seance
            INNER JOIN cinema ON cinema.id = seance.cinema_id
            INNER JOIN film ON film.id = seance.film_id
            {where_clause}
            ORDER BY datetime ASC",
        ))?;
        let rows = stmt.query_map([], |row| {
            Ok(QueryResult {
                cinema: Cinema {
                    id: row.get(1)?,
                    name: row.get(6)?,
                    url_path: row.get(7)?,
                    address: row.get(8)?,
                    image_path: row.get(9)?,
                },
                film: Film {
                    id: row.get(2)?,
                    name: row.get(10)?,
                    url_path: row.get(11)?,
                    image_path: row.get(12)?,
                    director: row.get(13)?,
                    release_date: row.get(14)?,
                },
                seance: Seance {
                    id: row.get(0)?,
                    cinema_id: row.get(1)?,
                    film_id: row.get(2)?,
                    datetime: row.get(3)?,
                    version: row.get(4)?,
                    url: row.get(5)?,
                },
            })
        })?;
        rows.collect()
    }

    fn get_seance(&self, id: u64) -> rusqlite::Result<Option<QueryResult>> {
        let mut stmt = self.prepare_cached(
            "SELECT
                seance.id, cinema_id, film_id, datetime, version, url,
                cinema.name, cinema.url_path, cinema.address, cinema.image_path,
                film.name, film.url_path, film.image_path, film.director, film.release_date
            FROM seance
            INNER JOIN cinema ON cinema.id = seance.cinema_id
            INNER JOIN film ON film.id = seance.film_id
            WHERE seance.id = ?1
            ORDER BY datetime ASC",
        )?;
        let mut rows = stmt.query_map([id], |row| {
            Ok(QueryResult {
                cinema: Cinema {
                    id: row.get(1)?,
                    name: row.get(6)?,
                    url_path: row.get(7)?,
                    address: row.get(8)?,
                    image_path: row.get(9)?,
                },
                film: Film {
                    id: row.get(2)?,
                    name: row.get(10)?,
                    url_path: row.get(11)?,
                    image_path: row.get(12)?,
                    director: row.get(13)?,
                    release_date: row.get(14)?,
                },
                seance: Seance {
                    id: row.get(0)?,
                    cinema_id: row.get(1)?,
                    film_id: row.get(2)?,
                    datetime: row.get(3)?,
                    version: row.get(4)?,
                    url: row.get(5)?,
                },
            })
        })?;
        rows.next().transpose()
    }
}

fn parse_date(date: &str) -> NaiveDate {
    let (day, month) = date
        .split_once('/')
        .expect("Date should be in format DD/MM");
    let day = day.parse::<u32>().unwrap();
    let month = month.parse::<u32>().unwrap();
    let date = NaiveDate::from_ymd_opt(NOW.year(), month, day).unwrap();
    if date < NOW.date_naive() {
        NaiveDate::from_ymd_opt(NOW.year() + 1, month, day).unwrap()
    } else {
        date
    }
}

fn parse_time(time: &str) -> NaiveTime {
    NaiveTime::parse_from_str(time, "%H:%M").expect("Time should be in format HH:MM")
}

#[derive(Args, Debug)]
struct ScrapeArgs {
    /// Database file path
    #[arg(long, default_value = DEFAULT_DB_PATH.display().to_string())]
    db_path: PathBuf,
}

async fn scrape(args: ScrapeArgs) {
    let progress = MultiProgress::new();

    let future_cinemas = async {
        let prog = progress.add(ProgressBar::new_spinner().with_message("Downloading cinemas"));
        prog.enable_steady_tick(Duration::from_millis(100));
        let mut cinemas: Vec<Cinema> = reqwest::get(CINEMAS_URL.as_str())
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        for (id, cinema) in cinemas.iter_mut().enumerate() {
            cinema.id = id as u64 + 1;
        }
        prog.disable_steady_tick();
        prog.finish_with_message("Downloaded cinemas");
        cinemas
    };
    let future_films = async {
        let prog = progress.add(ProgressBar::new_spinner().with_message("Downloading films"));
        prog.enable_steady_tick(Duration::from_millis(100));
        let films: Vec<Film> = reqwest::get(FILMS_URL.as_str())
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        prog.disable_steady_tick();
        prog.finish_with_message("Downloaded films");
        films
    };
    let (cinemas, films) = futures::future::join(future_cinemas, future_films).await;

    let seances = Arc::new(Mutex::new(Vec::<Seance>::new()));
    futures::future::join_all(cinemas.iter().map(|cinema| async {
        let prog = progress.add(
            ProgressBar::new_spinner()
                .with_message(format!("Downloading sceances: {}", cinema.name)),
        );
        prog.enable_steady_tick(Duration::from_millis(100));
        let cinema_html = reqwest::get(cinema.url())
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let cinema_soup = Soup::new(&cinema_html);
        prog.disable_steady_tick();
        prog.set_style(PROG_BAR_STYLE.clone());
        prog.set_message(cinema.name.clone());
        prog.set_length(cinema_soup.class("session-date").find_all().count() as u64);
        for film_soup in cinema_soup.class("movie-results-container").find_all() {
            let url_path = film_soup
                .class("poster")
                .find()
                .unwrap()
                .get("href")
                .unwrap();
            let film = films.iter().find(|f| f.url_path == url_path).unwrap();
            for seance_soup in film_soup.class("session-date").find_all() {
                let date = seance_soup
                    .class("sessionDate")
                    .find()
                    .unwrap()
                    .text()
                    .trim()
                    .split_once(' ')
                    .unwrap()
                    .1
                    .to_string();
                let time = seance_soup
                    .class("time")
                    .find()
                    .unwrap()
                    .text()
                    .trim()
                    .to_string();
                let datetime = NaiveDateTime::new(parse_date(&date), parse_time(&time))
                    .and_local_timezone(*PARIS_OFFSET)
                    .earliest()
                    .unwrap();
                let version = seance_soup
                    .class("version")
                    .find()
                    .unwrap()
                    .text()
                    .trim()
                    .to_string();
                let url = seance_soup
                    .tag("a")
                    .find()
                    .and_then(|link| link.get("href"));
                let mut seances = seances.lock().await;
                let exists = seances.iter().any(|s| {
                    s.cinema_id == cinema.id
                        && s.film_id == film.id
                        && s.datetime == datetime
                        && s.version == version
                        && s.url == url
                });
                if !exists {
                    let id = seances.len() as u64 + 1;
                    seances.push(Seance {
                        id,
                        cinema_id: cinema.id,
                        film_id: film.id,
                        datetime,
                        version,
                        url,
                    });
                }
                // conn.insert_seance(&seance).unwrap();
                prog.inc(1);
            }
        }
        prog.finish();
    }))
    .await;

    Database::delete(&args.db_path);
    let db = Database::open(&args.db_path);
    let conn = db.conn().unwrap();

    let prog = progress.add(
        ProgressBar::new(cinemas.len() as u64)
            .with_style(PROG_BAR_STYLE.clone())
            .with_message("Inserting cinemas"),
    );
    conn.create_cinemas().unwrap();
    for cinema in &cinemas {
        conn.insert_cinema(cinema).unwrap();
        prog.inc(1);
    }
    prog.finish_with_message("Inserted cinemas");

    let prog = progress.add(
        ProgressBar::new(films.len() as u64)
            .with_style(PROG_BAR_STYLE.clone())
            .with_message("Inserting films"),
    );
    conn.create_films().unwrap();
    for film in &films {
        conn.insert_film(film).unwrap();
        prog.inc(1);
    }
    prog.finish_with_message("Inserted films");

    let seances = seances.lock().await;
    let prog = progress.add(
        ProgressBar::new(seances.len() as u64)
            .with_style(PROG_BAR_STYLE.clone())
            .with_message("Inserting seances"),
    );
    conn.create_seances().unwrap();
    for seance in seances.iter() {
        conn.insert_seance(seance).unwrap();
        prog.inc(1);
    }
    prog.finish_with_message("Inserted seances");
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum GroupBy {
    Cinema,
    Film,
}

#[derive(Args, Debug)]
struct QueryArgs {
    /// Database file path
    #[arg(long, default_value = DEFAULT_DB_PATH.display().to_string())]
    db_path: PathBuf,
    /// Day to query DD/MM
    #[arg(long, short = 'd')]
    day: Option<String>,
    /// Time to query after HH:MM
    #[arg(long, short = 't')]
    time: Option<String>,
    /// Show VF only
    #[arg(long)]
    vf: bool,
    /// Show VO only
    #[arg(long)]
    vo: bool,
    /// Group by cinemas or films
    #[arg(long, short = 'g', value_enum, default_value_t = GroupBy::Cinema)]
    group: GroupBy,
}

type Grouping = BTreeMap<u64, (String, BTreeMap<u64, (String, Vec<QueryResult>)>)>;

fn style_id(id: u64) -> ANSIGenericString<'static, str> {
    Style::new().dimmed().paint(format!("[{id}]"))
}

async fn query(args: QueryArgs) {
    let options = QueryOptions {
        day: args.day.as_ref().map(|d| parse_date(d)),
        time: args.time.as_ref().map(|t| parse_time(t)),
        version: if args.vf && !args.vo {
            Some(Version::French)
        } else if !args.vf && args.vo {
            Some(Version::Original)
        } else {
            None
        },
    };
    let db = Database::open(&args.db_path);
    let conn = db.conn().unwrap();
    let mut grouping = Grouping::new();
    for result in conn.query_seances(options).unwrap() {
        match args.group {
            GroupBy::Cinema => grouping
                .entry(result.cinema.id)
                .or_insert_with(|| (result.cinema.description(), BTreeMap::new()))
                .1
                .entry(result.film.id)
                .or_insert_with(|| (result.film.description(), Vec::new()))
                .1
                .push(result),
            GroupBy::Film => grouping
                .entry(result.film.id)
                .or_insert_with(|| (result.film.description(), BTreeMap::new()))
                .1
                .entry(result.cinema.id)
                .or_insert_with(|| (result.cinema.description(), Vec::new()))
                .1
                .push(result),
        }
    }
    for (id, (description, group)) in grouping {
        println!(
            "{} {}\n",
            style_id(id),
            Style::new().bold().paint(description)
        );
        for (id, (description, results)) in group {
            println!("  {} {}", style_id(id), description);
            print!("   ");
            for result in results {
                print!(
                    " {} {} ({})",
                    style_id(result.seance.id),
                    if args.day.is_none() && args.time.is_none() {
                        result.seance.datetime.format("%d/%m %H:%M")
                    } else {
                        result.seance.datetime.format("%H:%M")
                    },
                    result.seance.version
                );
            }
            println!("\n");
        }
    }
}

#[derive(Args, Debug)]
pub struct SeanceArgs {
    /// Seance ID
    id: u64,
    /// Database file path
    #[arg(long, default_value = DEFAULT_DB_PATH.display().to_string())]
    db_path: PathBuf,
}

async fn seance(args: SeanceArgs) {
    let db = Database::open(&args.db_path);
    let conn = db.conn().unwrap();
    let result = if let Some(result) = conn.get_seance(args.id).unwrap() {
        result
    } else {
        println!("Seance {} not found", style_id(args.id));
        return;
    };
    println!("{}", style_id(result.seance.id),);
    println!("Film:    {}", result.film.description());
    println!("         {}", result.film.director);
    println!("         {}", result.film.url());
    println!("Cinema:  {}", result.cinema.name);
    println!("         {}", result.cinema.address);
    println!("         {}", result.cinema.url());
    println!("Version: {}", result.seance.version);
    println!("Date:    {}", result.seance.datetime.format("%b %d"));
    println!("Time:    {}", result.seance.datetime.format("%H:%M"));
    if let Some(url) = result.seance.url {
        println!("Reserve: {url}");
    }
}

async fn clean(args: ScrapeArgs) {
    Database::delete(args.db_path);
}

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Scrape cip-paris.fr and insert data into the database
    Scrape(ScrapeArgs),
    /// Query the database
    Query(QueryArgs),
    /// Get information about a seance
    Seance(SeanceArgs),
    /// Delete database
    Clean(ScrapeArgs),
}

#[tokio::main]
async fn main() {
    let args: Cli = Cli::parse();
    match args.command {
        Commands::Scrape(args) => scrape(args).await,
        Commands::Query(args) => query(args).await,
        Commands::Seance(args) => seance(args).await,
        Commands::Clean(args) => clean(args).await,
    }
}
