import json
import os
from collections import defaultdict
from config import (
    ENRICHED_DIR,
    DYNAMO_DIR,
    MAX_CAST,
    MAX_RECOMMENDATIONS,
    EPISODE_OVERVIEW_MAX_LEN,
    MOVIE_GENRES,
    TV_GENRES,
    CATALOG_FILE,
    MOVIE_DETAILS_FILE,
    TV_DETAILS_FILE,
    TV_SEASONS_FILE,
)
from utils.log import info, done


DATA_DIR = ENRICHED_DIR
OUTPUT_FILE = DYNAMO_DIR / CATALOG_FILE


def format_popularity(popularity: float) -> str:
    return f"{popularity:07.4f}"


def format_vote(vote: float) -> str:
    return f"{vote:05.2f}"


def normalize_date(d: str | None) -> str | None:
    if not d or not isinstance(d, str):
        return None
    d = d.strip()
    if len(d) != 10:
        return None
    return d


def build_category_item(
    *,
    pk: str,
    sk_prefix: str,
    score_str: str,
    content_type: str,
    content_id: int,
    title: str | None,
    name: str | None,
    poster_path: str | None,
    backdrop_path: str | None,
    tagline: str | None,
):
    item = {
        "PK": pk,
        "SK": f"{sk_prefix}#{score_str}#TITLE#{content_type}#{content_id}",
        "id": content_id,
    }

    if content_type:
        item["type"] = content_type
    if title:
        item["title"] = title
    if name:
        item["name"] = name
    if poster_path:
        item["poster_path"] = poster_path
    if backdrop_path:
        item["backdrop_path"] = backdrop_path
    if tagline:
        item["tagline"] = tagline

    return item


def load_details(path):
    data = {}
    full_path = os.path.join(DATA_DIR, path)

    if not os.path.exists(full_path):
        return data

    with open(full_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            obj = json.loads(line)
            cid = obj.get("id")

            if cid is not None:
                data[cid] = obj.get("data", {})

    return data


def load_tv_seasons(path):
    data = defaultdict(dict)
    full_path = os.path.join(DATA_DIR, path)

    if not os.path.exists(full_path):
        return data

    with open(full_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            obj = json.loads(line)
            tv_id = obj.get("tv_id")
            season_number = obj.get("season_number")

            if tv_id is None or season_number is None:
                continue

            data[tv_id][season_number] = obj.get("data", {})

    return {tv_id: list(seasons.values()) for tv_id, seasons in data.items()}


def build_movie_meta_item(details):
    cid = details.get("id")

    if cid is None:
        return None

    return {
        "PK": f"TITLE#movie#{cid}",
        "SK": "META",
        "type": "movie",
        "id": cid,
        "title": details.get("title"),
        "original_title": details.get("original_title"),
        "tagline": details.get("tagline"),
        "status": details.get("status"),
        "origin_country": details.get("origin_country") or [],
        "original_language": details.get("original_language"),
        "release_date": details.get("release_date"),
        "genres": details.get("genres") or [],
        "runtime": details.get("runtime"),
        "overview": details.get("overview"),
        "poster_path": details.get("poster_path"),
        "backdrop_path": details.get("backdrop_path"),
        "popularity": details.get("popularity"),
        "vote_average": details.get("vote_average"),
    }


def build_tv_meta_item(details):
    cid = details.get("id")

    if cid is None:
        return None

    return {
        "PK": f"TITLE#tv#{cid}",
        "SK": "META",
        "type": "tv",
        "id": cid,
        "title": details.get("name"),
        "original_title": details.get("original_name"),
        "tagline": details.get("tagline"),
        "status": details.get("status"),
        "origin_country": details.get("origin_country") or [],
        "original_language": details.get("original_language"),
        "first_air_date": details.get("first_air_date"),
        "genres": details.get("genres") or [],
        "number_of_seasons": details.get("number_of_seasons"),
        "overview": details.get("overview"),
        "poster_path": details.get("poster_path"),
        "backdrop_path": details.get("backdrop_path"),
        "popularity": details.get("popularity"),
        "vote_average": details.get("vote_average"),
    }


def build_credits_item(content_type, cid, credits):
    if credits is None:
        return None

    raw_cast = credits.get("cast") or []

    raw_cast = sorted(
        raw_cast,
        key=lambda c: c.get("order", 9999)
    )[:MAX_CAST]

    cast_out = []

    for c in raw_cast:
        cast_out.append({
            "id": c.get("id"),
            "name": c.get("name"),
            "character": c.get("character"),
            "profile_path": c.get("profile_path"),
        })

    return {
        "PK": f"TITLE#{content_type}#{cid}",
        "SK": "CREDITS",
        "cast": cast_out,
    }


def build_similar_item(content_type, cid, similar_list, valid_ids):

    if not similar_list:
        return None

    results_out = []

    for s in similar_list:

        sid = s.get("id")

        if sid not in valid_ids:
            continue

        if content_type == "movie":
            results_out.append({
                "id": sid,
                "type": "movie",
                "title": s.get("title") or s.get("original_title"),
                "poster_path": s.get("poster_path"),
            })
        else:
            results_out.append({
                "id": sid,
                "type": "tv",
                "title": s.get("name") or s.get("original_name"),
                "poster_path": s.get("poster_path"),
            })

    results_out = results_out[:MAX_RECOMMENDATIONS]

    if not results_out:
        return None

    return {
        "PK": f"TITLE#{content_type}#{cid}",
        "SK": "RECOMMENDATIONS",
        "results": results_out,
    }


def build_season_items(tv_id, seasons):

    items = []

    for s in seasons:

        season_number = s.get("season_number")

        if season_number is None:
            continue

        episodes = s.get("episodes") or []

        eps_out = []

        for e in episodes:

            overview = e.get("overview") or ""

            if len(overview) > EPISODE_OVERVIEW_MAX_LEN:
                overview = overview[:EPISODE_OVERVIEW_MAX_LEN]

            eps_out.append({
                "episode_number": e.get("episode_number"),
                "title": e.get("name"),
                "air_date": e.get("air_date"),
                "overview": overview,
                "still_path": e.get("still_path"),
                "runtime": e.get("runtime"),
            })

        items.append({
            "PK": f"TITLE#tv#{tv_id}",
            "SK": f"SEASON#{season_number}",
            "season_number": season_number,
            "air_date": s.get("air_date"),
            "episodes": eps_out,
        })

    return items


def generate_movie_category_items(details):
    items = []

    cid = details.get("id")
    if cid is None:
        return items

    popularity = details.get("popularity")
    vote_avg = details.get("vote_average")

    if popularity is None or vote_avg is None:
        return items

    pop_str = format_popularity(popularity)
    vote_str = format_vote(vote_avg)

    common = dict(
        content_type="movie",
        content_id=cid,
        title=details.get("title"),
        name=None,
        poster_path=details.get("poster_path"),
        backdrop_path=details.get("backdrop_path"),
        tagline=details.get("tagline"),
    )

    items.append(build_category_item(
        pk="CAT#movie#trending",
        sk_prefix="POP",
        score_str=pop_str,
        **common,
    ))

    items.append(build_category_item(
        pk="CAT#movie#top-rated",
        sk_prefix="RATE",
        score_str=vote_str,
        **common,
    ))

    release_date = normalize_date(details.get("release_date"))
    if release_date:
        items.append(build_category_item(
            pk="CAT#movie#latest",
            sk_prefix="DATE",
            score_str=release_date,
            **common,
        ))

    for g in details.get("genres") or []:
        gid = g.get("id")
        if gid in MOVIE_GENRES:
            items.append(build_category_item(
                pk=f"CAT#movie#genre#{gid}",
                sk_prefix="POP",
                score_str=pop_str,
                **common,
            ))

    return items


def generate_tv_category_items(details):
    items = []

    cid = details.get("id")
    if cid is None:
        return items

    popularity = details.get("popularity")
    vote_avg = details.get("vote_average")

    if popularity is None or vote_avg is None:
        return items

    pop_str = format_popularity(popularity)
    vote_str = format_vote(vote_avg)

    common = dict(
        content_type="tv",
        content_id=cid,
        title=details.get("name"),
        name=None,
        poster_path=details.get("poster_path"),
        backdrop_path=details.get("backdrop_path"),
        tagline=details.get("tagline"),
    )

    items.append(build_category_item(
        pk="CAT#tv#trending",
        sk_prefix="POP",
        score_str=pop_str,
        **common,
    ))

    items.append(build_category_item(
        pk="CAT#tv#top-rated",
        sk_prefix="RATE",
        score_str=vote_str,
        **common,
    ))

    first_air_date = normalize_date(details.get("first_air_date"))
    if first_air_date:
        items.append(build_category_item(
            pk="CAT#tv#latest",
            sk_prefix="DATE",
            score_str=first_air_date,
            **common,
        ))

    for g in details.get("genres") or []:
        gid = g.get("id")
        if gid in TV_GENRES:
            items.append(build_category_item(
                pk=f"CAT#tv#genre#{gid}",
                sk_prefix="POP",
                score_str=pop_str,
                **common,
            ))

    return items


def main():
    movie_details = load_details(MOVIE_DETAILS_FILE)
    tv_details = load_details(TV_DETAILS_FILE)

    tv_seasons = load_tv_seasons(TV_SEASONS_FILE)

    valid_ids = set(movie_details.keys()) | set(tv_details.keys())

    total_items = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as fout:

        for cid, details in movie_details.items():

            meta_item = build_movie_meta_item(details)

            if meta_item:
                fout.write(json.dumps(meta_item) + "\n")
                total_items += 1

            for cat_item in generate_movie_category_items(details):
                fout.write(json.dumps(cat_item) + "\n")
                total_items += 1

            credits_item = build_credits_item(
                "movie", cid, details.get("credits"))

            if credits_item:
                fout.write(json.dumps(credits_item) + "\n")
                total_items += 1

            sim_list = (details.get("recommendations")
                        or {}).get("results", [])

            similar_item = build_similar_item(
                "movie",
                cid,
                sim_list,
                valid_ids,
            )

            if similar_item:
                fout.write(json.dumps(similar_item) + "\n")
                total_items += 1

        for cid, details in tv_details.items():

            meta_item = build_tv_meta_item(details)

            if meta_item:
                fout.write(json.dumps(meta_item) + "\n")
                total_items += 1

            for cat_item in generate_tv_category_items(details):
                fout.write(json.dumps(cat_item) + "\n")
                total_items += 1

            credits_item = build_credits_item(
                "tv", cid, details.get("credits"))

            if credits_item:
                fout.write(json.dumps(credits_item) + "\n")
                total_items += 1

            sim_list = (details.get("recommendations")
                        or {}).get("results", [])

            similar_item = build_similar_item(
                "tv",
                cid,
                sim_list,
                valid_ids,
            )

            if similar_item:
                fout.write(json.dumps(similar_item) + "\n")
                total_items += 1

            seasons = tv_seasons.get(cid, [])

            for si in build_season_items(cid, seasons):
                fout.write(json.dumps(si) + "\n")
                total_items += 1

    info(f"Wrote {total_items} items to {OUTPUT_FILE}")
    done()


if __name__ == "__main__":
    main()
