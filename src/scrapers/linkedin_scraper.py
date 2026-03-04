"""Scraper LinkedIn via Crawl4AI (session authentifiée) + fallback Apify.

LinkedIn bloque activement le scraping. Deux stratégies :
1. Crawl4AI avec login (gratuit, mais risque de blocage)
2. Apify en fallback (promo code hackathon 20OUTIVY)

⚠️ Utilise un compte LinkedIn secondaire, pas ton compte principal.
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

load_dotenv()

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "keywords.json"

logger = logging.getLogger(__name__)


def load_linkedin_keywords():
    """Charge les mots-clés optimisés pour LinkedIn depuis keywords.json."""
    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)
    return config.get("linkedin_keywords", {}).get("keywords", [])


def build_search_urls(keywords):
    """Construit les URLs de recherche LinkedIn pour chaque mot-clé."""
    urls = []
    for kw in keywords:
        encoded = kw.replace(" ", "%20")
        urls.append({
            "url": f"https://www.linkedin.com/search/results/content/?keywords={encoded}&sortBy=%22date_posted%22",
            "keyword": kw,
        })
    return urls


async def scrape_linkedin_crawl4ai():
    """Scrape LinkedIn avec Crawl4AI via session authentifiée.

    Étape 1 : login avec les identifiants du .env
    Étape 2 : recherche de posts par mot-clé
    Étape 3 : extraction du markdown filtré
    """
    email = os.getenv("LINKEDIN_EMAIL")
    password = os.getenv("LINKEDIN_PASSWORD")

    if not email or not password:
        logger.warning("LinkedIn : pas d'identifiants dans .env, passage au fallback Apify")
        return None

    keywords = load_linkedin_keywords()
    search_urls = build_search_urls(keywords)

    logger.info("LinkedIn Crawl4AI : %d recherches à effectuer", len(search_urls))

    # Mode non-headless pour réduire la détection de bot
    browser_config = BrowserConfig(
        headless=False,
        viewport_width=1920,
        viewport_height=1080,
    )

    all_posts = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Étape 1 : login LinkedIn
        # Échappe les apostrophes pour éviter de casser le JS
        safe_email = email.replace("\\", "\\\\").replace("'", "\\'")
        safe_password = password.replace("\\", "\\\\").replace("'", "\\'")

        login_config = CrawlerRunConfig(
            session_id="linkedin_session",
            js_code=f"""
                document.querySelector('#username').value = '{safe_email}';
                document.querySelector('#password').value = '{safe_password}';
                document.querySelector('[type=submit]').click();
            """,
            wait_for="css:.global-nav",
            page_timeout=30000,
        )

        login_result = await crawler.arun(
            url="https://www.linkedin.com/login",
            config=login_config,
        )

        if not login_result.success:
            logger.error("LinkedIn : échec du login → %s", login_result.error_message)
            return None

        logger.info("LinkedIn : login réussi")
        await asyncio.sleep(3)

        # Étape 2 : recherche par mot-clé
        for entry in search_urls:
            try:
                search_config = CrawlerRunConfig(
                    session_id="linkedin_session",
                    wait_for="css:.feed-shared-update-v2",
                    page_timeout=20000,
                    js_code="window.scrollTo(0, document.body.scrollHeight);",
                )

                result = await crawler.arun(url=entry["url"], config=search_config)

                if not result.success:
                    logger.warning("LinkedIn : échec pour '%s'", entry["keyword"])
                    continue

                # Extraction du contenu en markdown
                content = str(result.markdown) if result.markdown else ""

                if content:
                    all_posts.append({
                        "keyword": entry["keyword"],
                        "content": content[:2000],
                        "source": "linkedin_crawl4ai",
                        "scraped_at": datetime.now().isoformat(),
                    })
                    logger.info("  LinkedIn '%s' → %d chars", entry["keyword"], len(content))

                # Pause longue pour éviter la détection
                await asyncio.sleep(5)

            except Exception as e:
                logger.error("LinkedIn erreur pour '%s' : %s", entry["keyword"], e)

    logger.info("LinkedIn Crawl4AI : %d résultats", len(all_posts))
    return all_posts


async def scrape_linkedin_apify():
    """Fallback : scrape LinkedIn via l'API Apify (promo code hackathon).

    Nécessite APIFY_KEY dans le .env.
    Actor utilisé : apify/linkedin-post-search-scraper
    """
    import httpx

    api_key = os.getenv("APIFY_KEY")
    if not api_key:
        logger.warning("Apify : pas de clé API dans .env, LinkedIn ignoré")
        return []

    keywords = load_linkedin_keywords()

    logger.info("LinkedIn Apify : lancement avec %d mots-clés", len(keywords))

    # Lance l'actor Apify
    async with httpx.AsyncClient(timeout=120) as client:
        run_response = await client.post(
            "https://api.apify.com/v2/acts/apify~linkedin-post-search-scraper/runs",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "keywords": keywords,
                "maxResults": 50,
            },
        )

        if run_response.status_code != 201:
            logger.error("Apify : erreur au lancement → %s", run_response.text)
            return []

        run_data = run_response.json()["data"]
        run_id = run_data["id"]
        logger.info("Apify : run lancé → %s", run_id)

        # Polling jusqu'à la fin du run
        for _ in range(30):
            await asyncio.sleep(10)
            status_resp = await client.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            status = status_resp.json()["data"]["status"]

            if status == "SUCCEEDED":
                break
            if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                logger.error("Apify : run échoué → %s", status)
                return []

        # Récupère les résultats
        dataset_resp = await client.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}/dataset/items",
            headers={"Authorization": f"Bearer {api_key}"},
        )

        items = dataset_resp.json()

        posts = []
        for item in items:
            posts.append({
                "title": item.get("text", "")[:200],
                "author": item.get("authorName", ""),
                "keyword": item.get("query", ""),
                "source": "linkedin_apify",
                "scraped_at": datetime.now().isoformat(),
            })

        logger.info("LinkedIn Apify : %d posts récupérés", len(posts))
        return posts


async def scrape_linkedin():
    """Orchestre le scraping LinkedIn : Crawl4AI d'abord, Apify en fallback."""
    # Essaie Crawl4AI en premier (gratuit)
    results = await scrape_linkedin_crawl4ai()

    # Si échec ou pas de résultats, fallback Apify
    if not results:
        logger.info("LinkedIn : fallback vers Apify")
        results = await scrape_linkedin_apify()

    return results or []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = asyncio.run(scrape_linkedin())
    print(json.dumps(results, indent=2, ensure_ascii=False)[:2000])
