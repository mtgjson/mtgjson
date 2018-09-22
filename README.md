# [**MTGJSON**](https://mtgjson.com/)

MTGJSON is an open sourced database creation and distribution tool for [*Magic: The Gathering*](https://magic.wizards.com/en) cards, specifically in [JSON](https://json.org/) format.

This repo contains our newest release, version 4. This version relies upon a variety of sources, such as Scryfall and Gatherer for our data.

To provide feedback and/or bug reports, please open a ticket as it is the best way for us to communicate with the public.  

If you would like to join or assist the development of the project, you can [join us on Discord](https://discord.gg/Hgyg7GJ) to discuss things further.

# How To Use

**Note:** These are the build directions to compile your own JSON files. If you are looking for pre-compiled JSON files, you can download them at [MTGJSON.com](https://mtgjson.com). 

This system was built using Python 3.7, so we can only guarantee proper functionality with this version.

1: To start, you'll need to install the MTGJSON4 package and dependencies. You can do this via:

```sh
$ pip3 install -r requirements.txt
$ python3 setup.py install
```

2: Select the flags you'd like to run the program with:

| Flags                      | Descriptions                                                                                                                                                                                           |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `-h`                       | This prints out the help menu and exits.                                                                                                                                                               |
| `-a`, `--all-sets`         | This tells the program to build all sets. This supersedes the `-s` flag.                                                                                                                               |
| `-s SET1 SET2 ...`         | This tells the program to build all set codes passed, provided they exist.                                                                                                                             |
| `-c`, `--compiled-outputs` | This tells the program to compile AllCards and AllSets, following any additional sets being built.                                                                                                     |
| `--skip-rebuild`           | This tells the program to build no sets, superseding `-a` and `-s`, and just use what is cached already. This is the equivalant of passing an empty `-s` flag. This is only useful with the `-c` flag. |
| `--skip-cached`            | This flag, in conjunction with `-a` or `-s`, tells the program to skips sets that have already been built, and build the remaining sets.                                                               |

3: Run the program via:
```sh
$ python3 mtgjson.py [-h] [-s SET [SET ...]] [-a] [-c] [--skip-rebuild] [--skip-cached]
```
