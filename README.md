# [**MTGJSON**](https://mtgjson.com/)

[![changelog](https://img.shields.io/badge/dynamic/json.svg?label=Version&url=https%3A%2F%2Fmtgjson.com%2Fjson%2Fversion.json&query=%24.version&colorB=blue)](https://mtgjson.com/changelog.html) ![Current Release Date](https://img.shields.io/badge/dynamic/json.svg?label=Released&url=https%3A%2F%2Fmtgjson.com%2Fjson%2Fversion.json&query=%24.date&colorB=blue)

MTGJSON is an open sourced database creation and distribution tool for [_Magic: The Gathering_](https://magic.wizards.com/) cards, specifically in [JSON](https://json.org/) format.
&nbsp;

## **Connect With Us**

Discord [![Discord](https://img.shields.io/discord/224178957103136779.svg)](https://discord.gg/74GUQDE)

Gitter [![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/mtgjson/mtgjson4)
___

## **About Us**

This repo contains our newest release, version `4.x`. This version relies upon a variety of sources, such as _Scryfall_ and _Gatherer_ for our data.

You can find our documentation with all properties [here](https://mtgjson.com/docs.html).

To provide feedback :information_desk_person: and/or bug reports :bug:, please [open a ticket](https://github.com/mtgjson/mtgjson4/issues/new/choose) :ticket: as it is the best way for us to communicate with the public.

If you would like to join or assist the development of the project, you can [join us on Discord](https://discord.gg/Hgyg7GJ) to discuss things further.

Looking for a full digest of pre-built data? Click [here](#notes).

&nbsp;
___

## **Dependencies**

- `Python 3.7`
  - Can be installed with at the official [Python Website](https://www.python.org/downloads/) or using [Homebrew](https://brew.sh/).
- `pip3`
  - Installed with `Python 3.7`. For troubleshooting check [here](https://stackoverflow.com/search?q=how+to+install+pip3).

&nbsp;
___

## **Installation**

Install the MTGJSON4 package and dependencies via:

```sh
pip3 install -r requirements.txt
```

&nbsp;
___

## **Output Flags**

| Flags                       | Flag Descriptions                                                                                                                              |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `-h, --help`                        | Print the help menu and exits.                                                                                                                  |
| `-a`                        | Build all sets. This overshadows the `-s` flag.                                                                                                 |
| `-c`                        | After building any/all sets, create the compiled outputs (ex: AllSets, AllCards).                                                               |
| `-x`                        | Skips sets that have already been built (i.e. set files in the output folder), and build the remaining sets. Must be passed with `-a` or `-s`. |
| `-s SET [SET ...]`          | Build set code arguments, provided they exist.                                                                                                  |
| `--skip-sets SET [SET ...]` | Prevents set code arguments from being built, even if passed in via `-a` or `-s`.                                                               |
| `--skip-tcgplayer`          | If you don't have a TCGPlayer API key, you can disable building of TCGPlayer components.                                                        |

> &nbsp;  
> **Newcomer Note**: Omitting either the `-a` and `-s` flag will yield empty outputted data.  
> &nbsp;

&nbsp;
___

## **Running**

Run the program, with any flag(s) you'd like, via:

```sh
python3 -m mtgjson4 [-h] [-acx] [-s [SET [SET ...]]] [--skip-tcgplayer] [--skip-sets [SET [SET ...]]]
```

Example:

```sh
$ python3 -m mtgjson4 -ac

> all outputted json files
```

___

## **Notes**

> &nbsp;  
> These are the build directions to compile your own JSON files but If you are looking for pre-compiled JSON files, you can download them at [MTGJSON.com](https://mtgjson.com/).  
> &nbsp;