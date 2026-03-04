"""Scraper Reddit via Crawl4AI sur old.reddit.com (pas de clé API nécessaire)."""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

# Charge la config des subreddits depuis le JSON
CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "subreddits.json"

logger = logging.getLogger(__name__)


def load_config():
    """Charge la liste des subreddits et les requêtes de recherche."""
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def build_urls(config):
    """Construit les URLs old.reddit.com à partir de la config.

    On cible old.reddit.com car le HTML est simple (pas de JS lourd).
    """
    urls = []

    # URLs des subreddits (posts récents)
    for group in ["fr_rh_paie", "en_rh_payroll"]:
        for sub in config[group]["subreddits"]:
            urls.append({
                "url": f"https://old.reddit.com/r/{sub}/new/",
                "source": f"r/{sub}",
                "type": "subreddit",
                "lang": "fr" if group == "fr_rh_paie" else "en",
            })

    # URLs de recherche par mot-clé
    for query in config.get("search_queries_fr", []):
        encoded = query.replace(" ", "+")
        urls.append({
            "url": f"https://old.reddit.com/search?q={encoded}&sort=new&t=month",
            "source": f"search:{query}",
            "type": "search",
            "lang": "fr",
        })

    return urls


# Schéma CSS pour extraire les posts depuis les pages subreddit (old.reddit.com)
SUBREDDIT_SCHEMA = {
    "name": "reddit_posts",
    "baseSelector": ".thing.link",
    "fields": [
        {"name": "title", "selector": "a.title", "type": "text"},
        {"name": "url", "selector": "a.title", "type": "attribute", "attribute": "href"},
        {"name": "score", "selector": ".score.unvoted", "type": "text"},
        {"name": "comments", "selector": ".comments", "type": "text"},
        {"name": "subreddit", "selector": ".subreddit", "type": "text"},
        {"name": "author", "selector": ".author", "type": "text"},
        {"name": "time", "selector": "time", "type": "attribute", "attribute": "datetime"},
    ],
}

# Schéma CSS pour les pages de recherche Reddit (structure HTML différente)
SEARCH_SCHEMA = {
    "name": "reddit_search_results",
    "baseSelector": ".search-result",
    "fields": [
        {"name": "title", "selector": "a.search-title", "type": "text"},
        {"name": "url", "selector": "a.search-title", "type": "attribute", "attribute": "href"},
        {"name": "subreddit", "selector": "a.search-subreddit-link", "type": "text"},
        {"name": "author", "selector": "a.author", "type": "text"},
        {"name": "time", "selector": "time", "type": "attribute", "attribute": "datetime"},
        {"name": "snippet", "selector": ".search-result-body", "type": "text"},
    ],
}


async def scrape_reddit():
    """Scrape les subreddits et recherches Reddit configurés.

    Retourne une liste de posts avec titre, score, commentaires, etc.
    """
    config = load_config()
    url_entries = build_urls(config)

    logger.info("Reddit : %d URLs à crawler", len(url_entries))

    # Configuration du navigateur (mode headless, user-agent classique)
    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
        viewport_width=1920,
        viewport_height=1080,
    )

    all_posts = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        for entry in url_entries:
            try:
                # Schéma différent selon le type de page (subreddit vs recherche)
                schema = SEARCH_SCHEMA if entry["type"] == "search" else SUBREDDIT_SCHEMA
                extraction = JsonCssExtractionStrategy(schema=schema)
                crawler_config = CrawlerRunConfig(
                    extraction_strategy=extraction,
                    page_timeout=30000,
                )

                result = await crawler.arun(url=entry["url"], config=crawler_config)

                if not result.success:
                    logger.warning("Échec pour %s : %s", entry["source"], result.error_message)
                    continue

                # Extraction des données structurées
                posts = json.loads(result.extracted_content) if result.extracted_content else []

                for post in posts:
                    post["source"] = entry["source"]
                    post["lang"] = entry["lang"]
                    post["scraped_at"] = datetime.now().isoformat()

                all_posts.extend(posts)
                logger.info("  %s → %d posts", entry["source"], len(posts))

                # Pause entre les requêtes pour éviter le rate limiting
                await asyncio.sleep(2)

            except Exception as e:
                logger.error("Erreur pour %s : %s", entry["source"], e)

    logger.info("Reddit : %d posts au total", len(all_posts))
    return all_posts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = asyncio.run(scrape_reddit())
    print(json.dumps(results, indent=2, ensure_ascii=False)[:2000])
