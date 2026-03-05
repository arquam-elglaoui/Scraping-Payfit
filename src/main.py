"""Point d'entrée principal du pipeline de social listening.

Orchestre les 4 scrapers + l'analyse OpenAI et génère les fichiers de sortie.
Usage : python src/main.py (depuis la racine ou depuis src/)
"""

import asyncio
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

# Fix encodage Windows : Crawl4AI utilise des caractères Unicode (↓, →)
# que le codec cp1252 de la console Windows ne supporte pas
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

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

    # --- Étape 1 : Scraping (tout en parallèle) ---

    logger.info("--- Scraping : Reddit + Forums + Trends + LinkedIn en parallèle ---")

    from scrapers.reddit_scraper import scrape_reddit
    from scrapers.forum_scraper import scrape_forums
    from scrapers.trends_scraper import scrape_trends
    from scrapers.linkedin_scraper import scrape_linkedin

    # Reddit, Forums et LinkedIn sont async → create_task
    reddit_task = asyncio.create_task(scrape_reddit())
    forums_task = asyncio.create_task(scrape_forums())
    linkedin_task = asyncio.create_task(scrape_linkedin())

    # Trends est synchrone → on le lance dans un thread pour ne pas bloquer
    trends_task = asyncio.get_event_loop().run_in_executor(None, scrape_trends)

    # Attend que tout finisse en parallèle
    raw_data["reddit"] = await reddit_task
    raw_data["forums"] = await forums_task
    raw_data["trends"] = await trends_task
    raw_data["linkedin"] = await linkedin_task

    # --- Étape 1b : Nettoyage des données ---

    logger.info("--- Nettoyage des données ---")

    # Dédoublonnage Reddit par titre (doublons entre subreddits et recherches)
    before_dedup = len(raw_data["reddit"])
    raw_data["reddit"] = deduplicate_posts(raw_data["reddit"])
    logger.info("  Reddit : %d → %d (-%d doublons)", before_dedup, len(raw_data["reddit"]), before_dedup - len(raw_data["reddit"]))

    # Filtre LinkedIn : garde uniquement les posts en français
    before_filter = len(raw_data["linkedin"])
    raw_data["linkedin"] = filter_relevant_posts(raw_data["linkedin"])
    logger.info("  LinkedIn : %d → %d pertinents (-%d hors cible)", before_filter, len(raw_data["linkedin"]), before_filter - len(raw_data["linkedin"]))

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


def _safe_int(value):
    """Convertit une valeur en int, retourne 0 si impossible."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def deduplicate_posts(posts):
    """Supprime les doublons Reddit en normalisant les titres.

    Deux posts avec le même titre (insensible à la casse, sans espaces extras)
    sont considérés comme des doublons. On garde celui avec le meilleur score.
    """
    seen = {}
    for post in posts:
        title = post.get("title", "").strip().lower()
        # Normalise les espaces multiples
        title = re.sub(r"\s+", " ", title)
        if not title:
            continue

        existing = seen.get(title)
        if existing is None:
            seen[title] = post
        else:
            # Garde le post avec le meilleur score
            current_score = _safe_int(post.get("score", 0))
            existing_score = _safe_int(existing.get("score", 0))
            if current_score > existing_score:
                seen[title] = post

    return list(seen.values())


# Caractères latins + accents FR + ponctuation courante
_FRENCH_PATTERN = re.compile(r"[a-zA-ZàâäéèêëïîôùûüÿçœæÀÂÄÉÈÊËÏÎÔÙÛÜŸÇŒÆ]")
# Caractères non-latins (arabe, chinois, cyrillique, etc.)
_NON_LATIN_PATTERN = re.compile(r"[\u0600-\u06FF\u0750-\u077F\u4E00-\u9FFF\u0400-\u04FF\u3040-\u309F\u30A0-\u30FF\uAC00-\uD7AF]")



# Marqueurs de pertinence PayFit France (droit du travail FR, paie FR, organismes FR)
# Insensible à la casse — couvre français ET anglais ("French labor law", "payroll France")
_FRANCE_MARKERS = {
    # Droit du travail français
    "smic", "urssaf", "cpam", "prud'hommes", "prudhommes", "prud'hommes",
    "code du travail", "droit du travail", "labor law france", "french labor",
    "convention collective", "accord de branche", "accord d'entreprise",
    "inspection du travail", "direccte", "dreets",
    # Contrats et dispositifs FR
    "cdi", "cdd", "dpae", "dsn", "rupture conventionnelle", "licenciement",
    "période d'essai", "période essai", "solde de tout compte",
    "indemnité licenciement", "indemnités", "préavis",
    # Paie et rémunération FR
    "fiche de paie", "bulletin de salaire", "bulletin de paie",
    "charges sociales", "charges patronales", "cotisations sociales",
    "salaire brut", "salaire net", "net imposable",
    "prime partage", "prime de partage", "ppv",
    "participation", "intéressement", "épargne salariale",
    "mutuelle entreprise", "prévoyance", "titres restaurant",
    # Congés et absences FR
    "congés payés", "congé payé", "rtt", "arrêt maladie",
    "congé maternité", "congé paternité", "congé parental",
    # Entreprises françaises
    "tpe", "pme", "sas", "sarl", "eurl", "auto-entrepreneur",
    "micro-entreprise", "france", "français", "française",
    # Outils et concurrents PayFit
    "payfit", "sage paie", "cegid", "silae", "lucca", "factorial",
    "nibelis", "sirh", "logiciel paie", "logiciel de paie",
    "automatisation paie", "digitalisation rh",
    # Tendances RH France
    "transparence salariale", "transparence des salaires",
    "index égalité", "égalité professionnelle",
    "télétravail", "remote france", "qvt", "qvct",
    "entretien professionnel", "entretien annuel",
    "formation professionnelle", "cpf", "opco",
    # Termes anglais pertinents pour PayFit FR
    "payroll france", "french payroll", "hr france", "french hr",
    "employee benefits france", "compliance france",
    "payroll software", "payroll automation", "hris",
}


def filter_relevant_posts(posts):
    """Filtre les posts LinkedIn pour ne garder que ceux pertinents PayFit France.

    Un post est pertinent s'il mentionne au moins un marqueur lié au droit
    du travail français, à la paie FR, ou à l'écosystème PayFit.
    Fonctionne quelle que soit la langue du post (FR, EN, ES...).
    Rejette les scripts non-latins (arabe, chinois, etc.).
    """
    filtered = []
    for post in posts:
        # Combine title + content pour ne rater aucun marqueur France
        content = " ".join(filter(None, [post.get("title", ""), post.get("content", "")]))
        if not content or len(content.strip()) < 20:
            continue

        # Rejette les scripts non-latins
        if _NON_LATIN_PATTERN.search(content):
            continue

        # Vérifie la pertinence France/PayFit
        content_lower = content.lower()
        if any(marker in content_lower for marker in _FRANCE_MARKERS):
            filtered.append(post)

    return filtered


def _build_methodology_section(configs, meta):
    """Construit la section méthodologie du rapport avec les détails techniques."""
    lines = [
        "## Methodologie",
        "",
        "### Pipeline de collecte",
        "",
        "Le pipeline orchestre 4 scrapers en parallele, nettoie les donnees, "
        "puis les analyse avec OpenAI GPT-4o-mini pour identifier les tendances emergentes.",
        "",
        "| Etape | Outil | Description |",
        "|-------|-------|-------------|",
        "| 1. Scraping Reddit | Crawl4AI | Extraction CSS sur old.reddit.com (HTML simple, pas de JS) |",
        "| 2. Google Trends | pytrends | Requetes emergentes (\"rising\") sur 90 jours, geo=FR |",
        "| 3. Forums RH | Crawl4AI + BM25 | Filtre de pertinence BM25 sur le contenu RH/paie |",
        "| 4. LinkedIn | Apify (harvestapi) | Recherche de posts par mots-cles, $0.002/post |",
        "| 5. Analyse | OpenAI GPT-4o-mini | Identification de 20 themes emergents en JSON structure |",
        "",
    ]

    # Subreddits
    subreddits_config = configs.get("subreddits.json", {})
    fr_subs = subreddits_config.get("fr_rh_paie", {}).get("subreddits", [])
    en_subs = subreddits_config.get("en_rh_payroll", {}).get("subreddits", [])
    search_queries = subreddits_config.get("search_queries_fr", [])

    lines.extend([
        "### Reddit – Sources et mots-cles",
        "",
        f"**Subreddits FR** ({len(fr_subs)}) : " + ", ".join(f"r/{s}" for s in fr_subs),
        "",
        f"**Subreddits EN** ({len(en_subs)}) : " + ", ".join(f"r/{s}" for s in en_subs),
        "",
        f"**Requetes de recherche** ({len(search_queries)}) :",
        "",
    ])
    for q in search_queries:
        lines.append(f"- `{q}`")
    lines.append("")

    # Google Trends
    keywords_config = configs.get("keywords.json", {})
    trends_groups = {
        k: v for k, v in keywords_config.items()
        if isinstance(v, dict) and "keywords" in v and k != "linkedin_keywords"
    }

    lines.extend([
        "### Google Trends – Groupes de mots-cles",
        "",
        f"**{len(trends_groups)} groupes** (max 5 mots-cles par groupe, geo=FR, periode=90 jours) :",
        "",
    ])
    for group_name, group_data in trends_groups.items():
        desc = group_data.get("description", "")
        kws = group_data.get("keywords", [])
        lines.append(f"- **{group_name}** ({desc}) : " + ", ".join(f"`{k}`" for k in kws))
    lines.append("")

    # Forums
    forum_urls = configs.get("forum_urls.json", [])
    lines.extend([
        "### Forums RH francais – Sites cibles",
        "",
        f"**{len(forum_urls)} URLs** crawlees avec filtre BM25 (seuil=0.3, requete: paie/salaire/conges/RH) :",
        "",
    ])
    for url in forum_urls:
        # Extrait le domaine pour lisibilité
        domain = url.split("//")[1].split("/")[0] if "//" in url else url
        lines.append(f"- [{domain}]({url})")
    lines.append("")

    # LinkedIn
    linkedin_kws = keywords_config.get("linkedin_keywords", {}).get("keywords", [])
    lines.extend([
        "### LinkedIn – Mots-cles de recherche",
        "",
        f"**{len(linkedin_kws)} requetes** via Apify (actor: harvestapi/linkedin-post-search) :",
        "",
    ])
    for kw in linkedin_kws:
        lines.append(f"- `{kw}`")
    lines.append("")

    # Nettoyage
    lines.extend([
        "### Nettoyage des donnees",
        "",
        "| Etape | Methode | Detail |",
        "|-------|---------|--------|",
        "| Deduplication Reddit | Normalisation titre (lowercase, espaces) | Garde le post avec le meilleur score |",
        "| Filtre LinkedIn | ~80 marqueurs France (droit du travail, paie, organismes FR) | Rejet des scripts non-latins + verification de pertinence France |",
        "| Filtre forums | BM25 | Ne garde que le contenu pertinent RH/paie (seuil 0.3) |",
        "",
        "**Marqueurs de pertinence France** (extrait) : "
        "`smic`, `urssaf`, `cpam`, `code du travail`, `cdi`, `cdd`, `rupture conventionnelle`, "
        "`fiche de paie`, `charges sociales`, `conges payes`, `tpe`, `pme`, `payfit`, "
        "`transparence salariale`, `teletravail`, `convention collective`, ...",
        "",
        "### Analyse IA",
        "",
        "- **Modele** : OpenAI GPT-4o-mini (temperature=0.3)",
        "- **Cout** : ~0.01€ par execution",
        "- **Prompt** : Expert SEO France, contexte PayFit (paie, conges, embauche pour TPE/PME)",
        "- **Sortie** : 20 themes emergents avec score, concurrence, intention, mots-cles SEO et angle PayFit",
    ])

    return lines


def _load_config_for_report():
    """Charge les configs pour la section méthodologie du rapport."""
    config_dir = Path(__file__).resolve().parents[1] / "config"
    configs = {}

    for name in ("keywords.json", "subreddits.json", "forum_urls.json"):
        path = config_dir / name
        if path.exists():
            with open(path, encoding="utf-8") as f:
                configs[name] = json.load(f)

    return configs


def generate_report(raw_data, topics):
    """Génère un rapport markdown enrichi pour l'équipe hackathon.

    Inclut un executive summary, la méthodologie détaillée, les stats clés,
    le top 5 prioritaire et les 20 thématiques complètes.
    """
    meta = raw_data.get("metadata", {})
    now = datetime.now().strftime("%d/%m/%Y à %H:%M")
    total_sources = (
        meta.get("total_reddit", 0)
        + meta.get("total_trends", 0)
        + meta.get("total_forums", 0)
        + meta.get("total_linkedin", 0)
    )

    # Statistiques sur les topics
    high_score = [t for t in topics if t.get("potential_score", 0) >= 8]
    low_competition = [t for t in topics if t.get("competition_level") == "faible"]
    transactional = [t for t in topics if t.get("search_intent") == "transactionnelle"]

    lines = [
        "# Rapport Social Listening – PayFit",
        f"\nGenere le {now}",
        "",
        "## Executive Summary",
        "",
        f"Ce rapport identifie **{len(topics)} thematiques emergentes** a partir de "
        f"**{total_sources} donnees** collectees sur 4 sources (Reddit, Google Trends, "
        f"forums RH francais, LinkedIn).",
        "",
        f"**Chiffres cles :**",
        f"- {len(high_score)} topics a fort potentiel (score >= 8/10)",
        f"- {len(low_competition)} topics a faible concurrence (opportunites quick-win)",
        f"- {len(transactional)} topics a intention transactionnelle (conversion directe)",
        "",
        "---",
        "",
        "## Sources collectees",
        "",
        "| Source | Nombre | Qualite |",
        "|--------|--------|---------|",
        f"| Reddit | {meta.get('total_reddit', 0)} posts | Dedoublonnes par titre |",
        f"| Google Trends | {meta.get('total_trends', 0)} mots-cles | Requetes emergentes |",
        f"| Forums RH | {meta.get('total_forums', 0)} discussions | Filtrees BM25 |",
        f"| LinkedIn | {meta.get('total_linkedin', 0)} posts | Filtres pertinence France |",
        f"| **Total** | **{total_sources}** | |",
        "",
        "---",
        "",
    ]

    # --- Section Méthodologie ---
    configs = _load_config_for_report()
    lines.extend(_build_methodology_section(configs, meta))
    lines.extend(["", "---", ""])

    # Top 5 prioritaire
    if topics:
        lines.extend([
            "## Top 5 – Actions prioritaires",
            "",
            "Les 5 sujets avec le meilleur ratio potentiel/concurrence :",
            "",
        ])

        # Trie par score desc, puis concurrence asc (faible > moyen > élevé)
        competition_order = {"faible": 0, "moyen": 1, "élevé": 2, "eleve": 2}
        sorted_topics = sorted(
            topics,
            key=lambda t: (
                -t.get("potential_score", 0),
                competition_order.get(t.get("competition_level", "moyen"), 1),
            ),
        )

        for i, topic in enumerate(sorted_topics[:5], 1):
            score = topic.get("potential_score", "?")
            competition = topic.get("competition_level", "?")
            title = topic.get("suggested_title", topic.get("topic", ""))
            keywords = ", ".join(topic.get("suggested_keywords", [])[:3])

            lines.extend([
                f"**{i}. {title}**",
                f"   Score: {score}/10 | Concurrence: {competition} | Mots-cles: `{keywords}`",
                "",
            ])

        lines.extend(["---", ""])

    # Les 20 thématiques complètes
    lines.extend([
        "## 20 thematiques emergentes (detail)",
        "",
    ])

    if not topics:
        lines.append("*Aucun topic identifie (verifier les donnees brutes et la cle OpenAI).*")
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
            sources = ", ".join(topic.get("sources", []))

            lines.extend([
                f"### {rank}. {title} (score: {score}/10)",
                f"- **Intention :** {intent}",
                f"- **Sources :** {sources}",
                f"- **Pourquoi ca monte :** {why}",
                f"- **Concurrence :** {competition}",
                f"- **Titre SEO suggere :** {suggested}",
                f"- **Mots-cles :** {keywords}",
                f"- **Angle PayFit :** {angle}",
                "",
            ])

    lines.extend([
        "---",
        "",
        "*Rapport genere automatiquement par le pipeline de social listening PayFit.*",
    ])

    return "\n".join(lines)


if __name__ == "__main__":
    asyncio.run(run_pipeline())
