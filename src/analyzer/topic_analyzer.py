"""Analyseur de topics via OpenAI API (GPT-4o-mini).

Prend les données brutes des scrapers et identifie les thématiques émergentes
avec un potentiel SEO pour PayFit.
"""

import json
import logging
import os

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

logger = logging.getLogger(__name__)

# Prompt optimisé pour l'extraction de topics émergents PayFit
ANALYSIS_PROMPT = """Tu es un expert SEO spécialisé dans le domaine RH et paie en France.

**Contexte :** PayFit est un logiciel SaaS de paie et RH ciblant les TPE/PME françaises.
Ses concurrents sont : Sage, Cegid, Lucca, Factorial.
Son contenu SEO couvre : fiche de paie, gestion du personnel, SIRH, congés, embauche, charges sociales.

**Ta mission :** À partir des données collectées (Reddit, LinkedIn, forums RH, Google Trends),
identifie les **20 thématiques émergentes** avec le plus fort potentiel SEO pour PayFit.

Pour chaque thématique, retourne un JSON structuré :
```json
[
  {
    "rank": 1,
    "topic": "Titre du sujet émergent",
    "why_emerging": "Pourquoi ce sujet monte (en 1 phrase)",
    "sources": ["reddit", "google_trends"],
    "search_intent": "informationnelle | transactionnelle | navigationnelle",
    "suggested_title": "Titre d'article SEO optimisé pour PayFit",
    "suggested_keywords": ["mot-clé 1", "mot-clé 2", "mot-clé 3"],
    "potential_score": 8,
    "competition_level": "faible | moyen | élevé",
    "payfit_angle": "Comment PayFit peut se positionner sur ce sujet"
  }
]
```

**Critères de sélection :**
- Priorise les sujets que PayFit ne couvre PAS encore
- Priorise les questions récurrentes sans bonne réponse en ligne
- Priorise les changements réglementaires récents
- Priorise les sujets où les concurrents sont absents
- Le score de potentiel (1-10) combine : volume estimé + tendance haussière + pertinence PayFit

**IMPORTANT :** Retourne UNIQUEMENT le JSON, sans texte autour.

Voici les données collectées :
"""


def prepare_data_summary(raw_data):
    """Prépare un résumé des données brutes pour le prompt.

    On tronque pour rester dans les limites de tokens (~10k tokens max).
    """
    summary_parts = []

    # Reddit
    reddit_posts = raw_data.get("reddit", [])
    if reddit_posts:
        summary_parts.append("=== REDDIT ===")
        for post in reddit_posts[:50]:
            title = post.get("title", "")
            source = post.get("source", "")
            score = post.get("score", "0")
            if title:
                summary_parts.append(f"[{source}] (score:{score}) {title}")

    # Google Trends
    trends = raw_data.get("trends", [])
    if trends:
        summary_parts.append("\n=== GOOGLE TRENDS (requêtes en hausse) ===")
        for trend in trends:
            keyword = trend.get("keyword", "")
            avg = trend.get("avg_interest", 0)
            rising = trend.get("rising_queries", [])
            if rising:
                rising_str = ", ".join(r.get("query", "") for r in rising[:5])
                summary_parts.append(f"[{keyword}] (intérêt:{avg}) → Émergents: {rising_str}")
            elif avg > 0:
                summary_parts.append(f"[{keyword}] (intérêt:{avg})")

    # Forums
    forum_posts = raw_data.get("forums", [])
    if forum_posts:
        summary_parts.append("\n=== FORUMS RH FRANÇAIS ===")
        for post in forum_posts[:30]:
            title = post.get("title", "")
            if title:
                summary_parts.append(f"- {title}")

    # LinkedIn
    linkedin_posts = raw_data.get("linkedin", [])
    if linkedin_posts:
        summary_parts.append("\n=== LINKEDIN ===")
        for post in linkedin_posts[:20]:
            title = post.get("title", post.get("content", ""))[:150]
            keyword = post.get("keyword", "")
            if title:
                summary_parts.append(f"[{keyword}] {title}")

    return "\n".join(summary_parts)


def analyze_topics(raw_data):
    """Envoie les données à GPT-4o-mini et retourne les topics émergents.

    Coût estimé : ~0,01€ par analyse (~5k tokens input, ~2k tokens output).
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY manquante dans le .env")
        return []

    client = OpenAI(api_key=api_key)

    # Résume les données pour tenir dans le contexte
    data_summary = prepare_data_summary(raw_data)
    logger.info("Analyse : %d caractères de données à analyser", len(data_summary))

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "user", "content": ANALYSIS_PROMPT + data_summary},
            ],
            temperature=0.3,
            max_tokens=4000,
        )

        content = response.choices[0].message.content.strip()

        # Nettoie le JSON (enlève les backticks markdown si présents)
        if content.startswith("```"):
            content = content.split("\n", 1)[1]
            content = content.rsplit("```", 1)[0]

        topics = json.loads(content)
        logger.info("Analyse : %d topics émergents identifiés", len(topics))
        return topics

    except json.JSONDecodeError as e:
        logger.error("Erreur parsing JSON OpenAI : %s", e)
        logger.debug("Réponse brute : %s", content)
        return []
    except Exception as e:
        logger.error("Erreur OpenAI API : %s", e)
        return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Test avec des données fictives
    test_data = {
        "reddit": [{"title": "Comment calculer les congés payés après un arrêt maladie ?", "source": "r/france", "score": "42"}],
        "trends": [{"keyword": "congés payés maladie", "avg_interest": 85, "rising_queries": [{"query": "congés payés arrêt maladie 2025", "value": 450}]}],
        "forums": [{"title": "Nouveau calcul congés payés après arrêt maladie"}],
        "linkedin": [],
    }
    results = analyze_topics(test_data)
    print(json.dumps(results, indent=2, ensure_ascii=False))
