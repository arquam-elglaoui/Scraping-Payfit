# Social Listening – Hackathon PayFit

Pipeline de détection de sujets émergents RH/paie pour créer du contenu SEO avant la concurrence.

## Stack

- **Crawl4AI** → scraping Reddit + forums RH + LinkedIn
- **pytrends** → Google Trends (requêtes en hausse)
- **OpenAI API** → analyse des topics (GPT-4o-mini)

## Installation

```bash
pip install -r requirements.txt
crawl4ai-setup
```

## Configuration

1. Copier `.env.example` → `.env`
2. Remplir les clés :
   - `OPENAI_API_KEY` (obligatoire)
   - `LINKEDIN_EMAIL` + `LINKEDIN_PASSWORD` (optionnel, compte secondaire)
   - `APIFY_KEY` (optionnel, fallback LinkedIn, promo code `20OUTIVY`)

## Lancer le pipeline

```bash
cd src
python main.py
```

## Output

Les résultats sont dans le dossier `output/` :

| Fichier | Contenu |
|---------|---------|
| `raw_data.json` | Données brutes de toutes les sources |
| `trending_topics.json` | 20 thématiques émergentes (JSON structuré) |
| `rapport.md` | Rapport lisible pour l'équipe |

## Structure

```
src/
├── scrapers/
│   ├── reddit_scraper.py      ← Crawl4AI sur old.reddit.com
│   ├── trends_scraper.py      ← pytrends (Google Trends FR)
│   ├── forum_scraper.py       ← Crawl4AI sur forums RH français
│   └── linkedin_scraper.py    ← Crawl4AI (auth) + Apify (fallback)
├── analyzer/
│   └── topic_analyzer.py      ← OpenAI GPT-4o-mini
└── main.py                    ← Orchestrateur principal
config/
├── subreddits.json            ← Subreddits + requêtes de recherche
├── keywords.json              ← Mots-clés par catégorie (8 groupes)
└── forum_urls.json            ← URLs des forums RH ciblés
```
