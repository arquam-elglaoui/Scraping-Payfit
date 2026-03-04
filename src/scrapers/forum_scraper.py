"""Scraper de forums RH français via Crawl4AI.

Cible les forums où les professionnels RH et comptables posent des questions
sur la paie et le droit du travail. Ces questions sont souvent en avance
sur les tendances Google (les gens demandent avant de chercher).
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import BM25ContentFilter

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "forum_urls.json"

logger = logging.getLogger(__name__)

# Mots-clés de filtrage pour ne garder que le contenu pertinent RH/paie
RELEVANCE_QUERY = "paie salaire congés embauche contrat travail RH employeur salarié"


def load_forum_urls():
    """Charge la liste des URLs de forums depuis le JSON."""
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


async def scrape_forums():
    """Scrape les forums RH français avec Crawl4AI.

    Utilise le filtre BM25 pour ne garder que le contenu pertinent
    (paie, RH, droit du travail). Le markdown généré est ensuite
    découpé en "discussions" individuelles.
    """
    forum_urls = load_forum_urls()

    logger.info("Forums : %d sites à crawler", len(forum_urls))

    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
        viewport_width=1920,
        viewport_height=1080,
    )

    # Filtre BM25 : ne garde que le contenu pertinent RH/paie
    content_filter = BM25ContentFilter(
        user_query=RELEVANCE_QUERY,
        bm25_threshold=1.0,
    )

    md_generator = DefaultMarkdownGenerator(content_filter=content_filter)

    crawler_config = CrawlerRunConfig(
        markdown_generator=md_generator,
        page_timeout=30000,
        remove_overlay_elements=True,
    )

    all_discussions = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        for url in forum_urls:
            try:
                result = await crawler.arun(url=url, config=crawler_config)

                if not result.success:
                    logger.warning("Échec pour %s : %s", url, result.error_message)
                    continue

                # Récupère le markdown filtré (contenu pertinent uniquement)
                markdown = result.markdown
                content = markdown.fit_markdown if hasattr(markdown, "fit_markdown") else str(markdown)

                if not content:
                    logger.warning("Pas de contenu pertinent sur %s", url)
                    continue

                # Découpe le markdown en blocs = discussions individuelles
                discussions = parse_discussions(content, url)
                all_discussions.extend(discussions)

                logger.info("  %s → %d discussions pertinentes", url, len(discussions))

                # Pause anti rate-limiting
                await asyncio.sleep(3)

            except Exception as e:
                logger.error("Erreur pour %s : %s", url, e)

    logger.info("Forums : %d discussions au total", len(all_discussions))
    return all_discussions


def parse_discussions(markdown_content, source_url):
    """Découpe le markdown en discussions individuelles.

    Chaque titre (## ou ###) dans le markdown = une discussion.
    """
    discussions = []
    lines = markdown_content.split("\n")

    current_title = None
    current_content = []

    for line in lines:
        # Détecte un nouveau titre de discussion
        if line.startswith("#"):
            # Sauvegarde la discussion précédente
            if current_title:
                discussions.append({
                    "title": current_title.strip(),
                    "content": "\n".join(current_content).strip()[:500],
                    "source_url": source_url,
                    "scraped_at": datetime.now().isoformat(),
                })

            current_title = line.lstrip("#").strip()
            current_content = []
        else:
            current_content.append(line)

    # Dernière discussion
    if current_title:
        discussions.append({
            "title": current_title.strip(),
            "content": "\n".join(current_content).strip()[:500],
            "source_url": source_url,
            "scraped_at": datetime.now().isoformat(),
        })

    return discussions


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = asyncio.run(scrape_forums())
    print(json.dumps(results, indent=2, ensure_ascii=False)[:2000])
