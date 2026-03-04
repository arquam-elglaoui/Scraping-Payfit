"""Scraper Google Trends via pytrends (gratuit, sans clé API).

pytrends interroge Google Trends pour récupérer :
- L'intérêt dans le temps (90 derniers jours)
- Les requêtes associées en hausse ("rising") → c'est ça le jackpot pour détecter les sujets émergents
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

from pytrends.request import TrendReq

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "keywords.json"

logger = logging.getLogger(__name__)


def load_keywords():
    """Charge les groupes de mots-clés depuis le JSON."""
    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)

    # On extrait uniquement les groupes qui ont des keywords (pas _description, pas linkedin_keywords)
    groups = {}
    for key, value in config.items():
        if isinstance(value, dict) and "keywords" in value and key != "linkedin_keywords":
            groups[key] = value["keywords"]

    return groups


def scrape_trends():
    """Interroge Google Trends par groupe de mots-clés.

    Retourne les tendances et requêtes associées en hausse.
    pytrends accepte max 5 mots-clés par requête → d'où le groupement dans keywords.json.
    """
    keyword_groups = load_keywords()

    # geo="FR" → recherches en France uniquement
    pytrends = TrendReq(hl="fr-FR", tz=60)

    all_trends = []

    # Délai de base entre les requêtes (secondes)
    base_delay = 15

    for group_name, keywords in keyword_groups.items():
        logger.info("Trends : groupe '%s' → %s", group_name, keywords)

        # Retry avec backoff exponentiel (max 2 tentatives)
        for attempt in range(3):
            try:
                # Recréer la session pytrends à chaque retry pour contourner le blocage
                if attempt > 0:
                    pytrends = TrendReq(hl="fr-FR", tz=60)

                pytrends.build_payload(keywords, cat=0, timeframe="today 3-m", geo="FR")

                interest = pytrends.interest_over_time()
                related = pytrends.related_queries()

                for keyword in keywords:
                    trend_entry = {
                        "keyword": keyword,
                        "group": group_name,
                        "scraped_at": datetime.now().isoformat(),
                        "rising_queries": [],
                        "top_queries": [],
                        "avg_interest": 0,
                    }

                    if not interest.empty and keyword in interest.columns:
                        trend_entry["avg_interest"] = int(interest[keyword].mean())

                    if keyword in related and related[keyword]["rising"] is not None:
                        rising_df = related[keyword]["rising"]
                        trend_entry["rising_queries"] = rising_df.to_dict("records")

                    if keyword in related and related[keyword]["top"] is not None:
                        top_df = related[keyword]["top"]
                        trend_entry["top_queries"] = top_df.to_dict("records")

                    all_trends.append(trend_entry)

                # Succès → pause normale avant le groupe suivant
                time.sleep(base_delay)
                break

            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    # Rate-limit → backoff exponentiel (30s, 60s)
                    wait = 30 * (2 ** attempt)
                    logger.warning("Trends 429 pour '%s', retry dans %ds...", group_name, wait)
                    time.sleep(wait)
                else:
                    logger.error("Erreur Trends pour '%s' : %s", group_name, e)
                    break

    logger.info("Trends : %d résultats au total", len(all_trends))
    return all_trends


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = scrape_trends()
    print(json.dumps(results, indent=2, ensure_ascii=False)[:2000])
