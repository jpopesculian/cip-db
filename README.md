# cip

I'm a big fan of les cinémas indépendants parisiens, but their website is pretty
unusable for me. I wanted something closer to
https://www.film.at/kinoprogramm/wien so I built a CLI that scrapes the data
from cip-paris.fr and displays it in a more readable way.

To install

```bash
cargo install --git https://github.com/jpopesculian/cip-db.git
```

To use

```bash
cip scrape # scrape the data and build the database
cip query --help # see the query options
cip seance <SEANCE_ID> # see the details of a seance you got from the query
```
