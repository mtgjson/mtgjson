# [**MTGJSON**](https://mtgjson.com/v4) ![](https://img.shields.io/badge/dynamic/json.svg?label=version&url=https%3A%2F%2Fmtgjson.com%2Fv4%2Fjson%2Fversion.json&query=%24.version&colorB=blue) ![](https://img.shields.io/badge/dynamic/json.svg?label=released&url=https%3A%2F%2Fmtgjson.com%2Fv4%2Fjson%2Fversion.json&query=%24.date&colorB=blue)

# Connect With Us
Discord via [![Discord](https://img.shields.io/discord/224178957103136779.svg)](https://discord.gg/74GUQDE)

Gitter via [![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/mtgjson/mtgjson4)

# About Us

MTGJSON is an open sourced database creation and distribution tool for [*Magic: The Gathering*](https://magic.wizards.com/) cards, specifically in [JSON](https://json.org/) format.

This repo contains our newest release, version 4. This version relies upon a variety of sources, such as *Scryfall* and *Gatherer* for our data.

You can find our documentation with all properties [here](https://mtgjson.com/v4/docs.html).

To provide feedback and/or bug reports, please [open a ticket](https://github.com/mtgjson/mtgjson4/issues/new/choose) as it is the best way for us to communicate with the public.

If you would like to join or assist the development of the project, you can [join us on Discord](https://discord.gg/Hgyg7GJ) to discuss things further.

# How To Use

>**Note:** These are the build directions to compile your own JSON files.<br>
>If you are looking for pre-compiled JSON files, you can download them at [MTGJSON.com](https://mtgjson.com/v4).

This system was built using *Python 3.7*, so we can only guarantee proper functionality with this version.


1. First, you'll need to install the MTGJSON4 package and dependencies. You can do this via:

```sh
$ pip3 install -r requirements.txt
```

2. Select the flags you'd like to run the program with:

| Flags                      | Descriptions                                                                                                                                                                                           |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `-h`                       | This prints out the help menu and exits.                                                                                                                                                               |
| `-a`, `--all-sets`         | This tells the program to build all sets. This supersedes the `-s` flag.                                                                                                                               |
| `-s SET1 SET2 ...`         | This tells the program to build all set codes passed, provided they exist.                                                                                                                             |
| `-c`, `--compiled-outputs` | This tells the program to compile AllCards and AllSets, following any additional sets being built. This flag needs to be accompanied by either `-a`, `-s`, or `--skip-rebuild`.                        |
| `--skip-rebuild`           | This tells the program to build no sets, superseding `-a` and `-s`, and just use what is cached already. This is the equivalent of passing an empty `-s` flag. This is only useful with the `-c` flag. |
| `--skip-cached`            | This flag, in conjunction with `-a` or `-s`, tells the program to skips sets that have already been built, and build the remaining sets.                                                               |

3. Run the program, with any flags you'd like, via:
```sh
usage: mtgjson4 [-h] [-s [SET [SET ...]]] [-a] [-c] [--skip-rebuild] [--skip-cached]
$ python3 -m mtgjson4
```
