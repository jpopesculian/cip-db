use chrono::{prelude::*, DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use soup::prelude::*;
use std::{path::PathBuf, sync::Arc, time::Duration};

lazy_static::lazy_static! {
    static ref ROOT_URL: Url = Url::parse("https://www.cip-paris.fr").unwrap();
    static ref FILMS_URL: Url = ROOT_URL.join("/json/movies").unwrap();
    static ref CINEMAS_URL: Url = ROOT_URL.join("/json/cinemas").unwrap();
    static ref PROG_BAR_STYLE: ProgressStyle =
                ProgressStyle::with_template("  {msg:26} {bar:40}   {pos}/{len}")
                    .unwrap();
    static ref PARIS_OFFSET: FixedOffset = chrono::FixedOffset::east_opt(2 * 3600).unwrap();
    static ref NOW: DateTime<FixedOffset> = Utc::now().with_timezone(&*PARIS_OFFSET);
}

#[derive(Serialize, Deserialize, Debug)]
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
    fn url(&self) -> Url {
        ROOT_URL.join(&self.url_path).unwrap()
    }
    // fn image(&self) -> Url {
    //     ROOT_URL.join(&self.image_path).unwrap()
    // }
}

#[derive(Serialize, Deserialize, Debug)]
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

// impl Film {
//     fn url(&self) -> Url {
//         ROOT_URL.join(&self.url_path).unwrap()
//     }
//     fn image(&self) -> Url {
//         ROOT_URL.join(&self.image_path).unwrap()
//     }
// }

#[derive(Serialize, Deserialize, Debug)]
struct Seance {
    cinema_id: u64,
    film_id: u64,
    datetime: DateTime<FixedOffset>,
    version: String,
    url: Option<String>,
}

pub struct Database(Arc<Pool<SqliteConnectionManager>>);

impl Database {
    pub fn path() -> PathBuf {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "data.db".to_string())
            .into()
    }
    pub fn open() -> Self {
        let path = std::env::var("DATABASE_URL").unwrap_or_else(|_| "data.db".to_string());
        let manager = SqliteConnectionManager::file(path);
        let pool = Pool::new(manager).unwrap();
        Self(Arc::new(pool))
    }

    pub fn delete() {
        if Self::path().exists() {
            std::fs::remove_file(Self::path()).unwrap();
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
        self.execute(
            "INSERT INTO cinema
                (id, name, url_path, address, image_path)
                VALUES (?1, ?2, ?3, ?4, ?5)",
            (
                cinema.id,
                &cinema.name,
                &cinema.url_path,
                &cinema.address,
                &cinema.image_path,
            ),
        )
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
        self.execute(
            "INSERT INTO film
                (id, name, url_path, image_path, director, release_date)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (
                film.id,
                &film.name,
                &film.url_path,
                &film.image_path,
                &film.director,
                &film.release_date,
            ),
        )
    }

    fn create_seances(&self) -> rusqlite::Result<usize> {
        self.execute(
            "CREATE TABLE seance (
                cinema_id INTEGER NOT NULL,
                film_id INTEGER NOT NULL,
                datetime TEXT NOT NULL,
                version TEXT NOT NULL,
                url TEXT
            )",
            (),
        )
    }

    fn insert_seance(&self, seance: &Seance) -> rusqlite::Result<usize> {
        self.execute(
            "INSERT INTO seance
                (cinema_id, film_id, datetime, version, url)
                VALUES (?1, ?2, ?3, ?4, ?5)",
            (
                seance.cinema_id,
                seance.film_id,
                &seance.datetime,
                &seance.version,
                &seance.url,
            ),
        )
    }
}

fn parse_date(date: &str) -> NaiveDate {
    let (day, month) = date.split_once('/').unwrap();
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
    NaiveTime::parse_from_str(time, "%H:%M").unwrap()
}

#[tokio::main]
async fn main() {
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

    Database::delete();
    let db = Database::open();
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

    conn.create_seances().unwrap();
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
        let films_soup = cinema_soup
            .class("movie-results-container")
            .find_all()
            .collect::<Vec<_>>();
        prog.disable_steady_tick();
        prog.set_style(PROG_BAR_STYLE.clone());
        prog.set_message(cinema.name.clone());
        prog.set_length(films_soup.len() as u64);
        for film_soup in films_soup {
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
                let seance = Seance {
                    cinema_id: cinema.id,
                    film_id: film.id,
                    datetime,
                    version,
                    url,
                };
                conn.insert_seance(&seance).unwrap();
            }
            prog.inc(1);
        }
        prog.finish();
    }))
    .await;
}
