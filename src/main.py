"""Point d'entrée principal du pipeline de social listening.

Orchestre les 4 scrapers + l'analyse OpenAI et génère les fichiers de sortie.
Usage : python src/main.py (depuis la racine ou depuis src/)
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Charge les variables d'environnement (.env) dès le départ
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

# Ajoute src/ au sys.path pour que les imports marchent depuis n'importe où
SRC_DIR = Path(__file__).resolve().parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Dossier de sortie pour les résultats
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"


async def run_pipeline():
    """Exécute le pipeline complet de social listening.

    1. Scrape Reddit, Google Trends, forums RH, LinkedIn (en parallèle quand possible)
    2. Consolide les données brutes dans raw_data.json
    3. Analyse avec OpenAI → trending_topics.json
    4. Génère un rapport markdown → rapport.md
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    logger = logging.getLogger("main")

    logger.info("=== Démarrage du pipeline de social listening ===")
    OUTPUT_DIR.mkdir(exist_ok=True)

    raw_data = {}

    # --- Étape 1 : Scraping (Reddit + forums en parallèle, puis LinkedIn) ---

    # Reddit et forums sont indépendants → on les lance en parallèle
    logger.info("--- Étape 1/4 : Scraping Reddit + Forums ---")

    from scrapers.reddit_scraper import scrape_reddit
    from scrapers.forum_scraper import scrape_forums

    reddit_task = asyncio.create_task(scrape_reddit())
    forums_task = asyncio.create_task(scrape_forums())

    raw_data["reddit"] = await reddit_task
    raw_data["forums"] = await forums_task

    # Google Trends (synchrone, pas d'async)
    logger.info("--- Étape 2/4 : Google Trends ---")

    from scrapers.trends_scraper import scrape_trends
    raw_data["trends"] = scrape_trends()

    # LinkedIn (le plus lent et risqué → en dernier)
    logger.info("--- Étape 3/4 : LinkedIn ---")

    from scrapers.linkedin_scraper import scrape_linkedin
    raw_data["linkedin"] = await scrape_linkedin()

    # --- Étape 2 : Sauvegarde des données brutes ---

    raw_data["metadata"] = {
        "scraped_at": datetime.now().isoformat(),
        "total_reddit": len(raw_data.get("reddit", [])),
        "total_trends": len(raw_data.get("trends", [])),
        "total_forums": len(raw_data.get("forums", [])),
        "total_linkedin": len(raw_data.get("linkedin", [])),
    }

    raw_path = OUTPUT_DIR / "raw_data.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=2, ensure_ascii=False)

    logger.info("Données brutes sauvegardées → %s", raw_path)
    logger.info(
        "  Reddit: %d | Trends: %d | Forums: %d | LinkedIn: %d",
        raw_data["metadata"]["total_reddit"],
        raw_data["metadata"]["total_trends"],
        raw_data["metadata"]["total_forums"],
        raw_data["metadata"]["total_linkedin"],
    )

    # --- Étape 3 : Analyse OpenAI ---

    logger.info("--- Étape 4/4 : Analyse OpenAI ---")

    from analyzer.topic_analyzer import analyze_topics
    topics = analyze_topics(raw_data)

    topics_path = OUTPUT_DIR / "trending_topics.json"
    with open(topics_path, "w", encoding="utf-8") as f:
        json.dump(topics, f, indent=2, ensure_ascii=False)

    logger.info("Topics émergents sauvegardés → %s (%d topics)", topics_path, len(topics))

    # --- Étape 4 : Génération du rapport markdown ---

    report = generate_report(raw_data, topics)
    report_path = OUTPUT_DIR / "rapport.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)

    logger.info("Rapport généré → %s", report_path)
    logger.info("=== Pipeline terminé ===")


def generate_report(raw_data, topics):
    """Génère un rapport markdown lisible pour l'équipe hackathon."""
    meta = raw_data.get("metadata", {})
    now = datetime.now().strftime("%d/%m/%Y à %H:%M")

    lines = [
        "# Rapport Social Listening – PayFit",
        f"\nGénéré le {now}",
        "",
        "## Sources collectées",
        "",
        f"| Source | Nombre |",
        f"|--------|--------|",
        f"| Reddit | {meta.get('total_reddit', 0)} posts |",
        f"| Google Trends | {meta.get('total_trends', 0)} mots-clés analysés |",
        f"| Forums RH | {meta.get('total_forums', 0)} discussions |",
        f"| LinkedIn | {meta.get('total_linkedin', 0)} posts |",
        "",
        "---",
        "",
        "## Top 20 thématiques émergentes",
        "",
    ]

    if not topics:
        lines.append("*Aucun topic identifié (vérifier les données brutes et la clé OpenAI).*")
    else:
        for topic in topics:
            rank = topic.get("rank", "?")
            title = topic.get("topic", "Sans titre")
            score = topic.get("potential_score", "?")
            intent = topic.get("search_intent", "?")
            why = topic.get("why_emerging", "")
            suggested = topic.get("suggested_title", "")
            keywords = ", ".join(topic.get("suggested_keywords", []))
            angle = topic.get("payfit_angle", "")
            competition = topic.get("competition_level", "?")

            lines.extend([
                f"### {rank}. {title} (score: {score}/10)",
                f"- **Intention :** {intent}",
                f"- **Pourquoi ça monte :** {why}",
                f"- **Concurrence :** {competition}",
                f"- **Titre SEO suggéré :** {suggested}",
                f"- **Mots-clés :** {keywords}",
                f"- **Angle PayFit :** {angle}",
                "",
            ])

    lines.extend([
        "---",
        "",
        "*Rapport généré automatiquement par le pipeline de social listening.*",
    ])

    return "\n".join(lines)


if __name__ == "__main__":
    asyncio.run(run_pipeline())
