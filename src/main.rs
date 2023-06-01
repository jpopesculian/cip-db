use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc, time::Duration};

lazy_static::lazy_static! {
    static ref ROOT_URL: Url = Url::parse("https://www.cip-paris.fr").unwrap();
    static ref FILMS_URL: Url = ROOT_URL.join("/json/movies").unwrap();
    static ref CINEMAS_URL: Url = ROOT_URL.join("/json/cinemas").unwrap();
    static ref PROG_BAR_STYLE: ProgressStyle =
                ProgressStyle::with_template("  {msg:20} {bar:40}   {pos}/{len}")
                    .unwrap();
}

#[derive(Serialize, Deserialize, Debug)]
struct Cinema {
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
    fn image(&self) -> Url {
        ROOT_URL.join(&self.image_path).unwrap()
    }
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

impl Film {
    fn url(&self) -> Url {
        ROOT_URL.join(&self.url_path).unwrap()
    }
    fn image(&self) -> Url {
        ROOT_URL.join(&self.image_path).unwrap()
    }
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
                (name, url_path, address, image_path)
                VALUES (?1, ?2, ?3, ?4)",
            (
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
                id INTEGER PRIMARY KEY,
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
}

#[tokio::main]
async fn main() {
    let progress = MultiProgress::new();

    let future_cinemas = async {
        let prog = progress.add(ProgressBar::new_spinner().with_message("Downloading cinemas"));
        prog.enable_steady_tick(Duration::from_millis(100));
        let cinemas: Vec<Cinema> = reqwest::get(CINEMAS_URL.as_str())
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
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
        // prog.set_message("Downloaded films");
        prog.finish_with_message("Downloaded films");
        films
    };
    let (cinemas, films) = futures::join!(future_cinemas, future_films);

    Database::delete();
    let db = Database::open();
    let conn = db.conn().unwrap();

    let prog = progress.add(
        ProgressBar::new(cinemas.len() as u64)
            .with_style(PROG_BAR_STYLE.clone())
            .with_message("Inserting cinemas"),
    );
    conn.create_cinemas().unwrap();
    for cinema in cinemas {
        conn.insert_cinema(&cinema).unwrap();
        prog.inc(1);
    }
    prog.finish_with_message("Inserted cinemas");

    let prog = progress.add(
        ProgressBar::new(films.len() as u64)
            .with_style(PROG_BAR_STYLE.clone())
            .with_message("Inserting films"),
    );
    conn.create_films().unwrap();
    for film in films {
        conn.insert_film(&film).unwrap();
        prog.inc(1);
    }
    prog.finish_with_message("Inserted films");
}
