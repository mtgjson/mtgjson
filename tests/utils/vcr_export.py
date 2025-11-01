"""
Helper utilities to export requests-cache responses to VCR cassettes.

Adapted from requests-cache documentation for VCR integration.
"""

from os import makedirs
from os.path import abspath, dirname, expanduser
from urllib.parse import urlparse

import yaml
from requests_cache import BaseCache, CachedResponse, __version__
from requests_cache.serializers.preconf import yaml_preconf_stage


def to_vcr_cassette(cache: BaseCache, path: str) -> None:
    """
    Export all cached responses to a VCR-compatible YAML cassette.

    :param cache: The requests_cache BaseCache instance
    :param path: Path to write the cassette YAML file
    """
    responses = cache.responses.values()
    write_cassette(to_vcr_cassette_dict(responses), path)


def to_vcr_cassette_dict(responses):
    """
    Convert cached responses to VCR cassette dictionary format.

    :param responses: Iterable of CachedResponse objects
    :return: Dictionary in VCR cassette format
    """
    return {
        "http_interactions": [to_vcr_episode(r) for r in responses],
        "recorded_with": f"requests-cache {__version__}",
    }


def to_vcr_episode(response: CachedResponse):
    """
    Convert a single CachedResponse to a VCR episode (interaction).

    :param response: CachedResponse to convert
    :return: Dictionary in VCR episode format
    """
    rd = yaml_preconf_stage.dumps(response)
    md = lambda d: {k: [v] for k, v in d.items()}
    return {
        "request": {
            "body": rd["request"]["body"],
            "headers": md(rd["request"]["headers"]),
            "method": rd["request"]["method"],
            "uri": rd["request"]["url"],
        },
        "response": {
            "body": {"string": rd["_content"], "encoding": rd["encoding"]},
            "headers": md(rd["headers"]),
            "status": {"code": rd["status_code"], "message": rd["reason"]},
            "url": rd["url"],
        },
        "recorded_at": rd["created_at"],
    }


def write_cassette(cassette, path):
    """
    Write a cassette dictionary to a YAML file.

    :param cassette: Dictionary in VCR cassette format
    :param path: Path to write the cassette file
    """
    path = abspath(expanduser(path))
    makedirs(dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(yaml.safe_dump(cassette))


def to_vcr_cassettes_by_host(cache: BaseCache, cassette_dir: str) -> None:
    """
    Export cached responses to separate cassette files by host.

    :param cache: The requests_cache BaseCache instance
    :param cassette_dir: Directory to write cassette files (one per host)
    """
    responses_by_host = {}
    for response in cache.responses.values():
        host = urlparse(response.url).netloc
        if host not in responses_by_host:
            responses_by_host[host] = []
        responses_by_host[host].append(response)

    for host, responses in responses_by_host.items():
        cassette_path = f"{cassette_dir}/{host}.yml"
        write_cassette(to_vcr_cassette_dict(responses), cassette_path)
