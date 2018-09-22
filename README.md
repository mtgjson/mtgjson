# [**MTGJSON**](https://mtgjson.com/)

MTGJSON is an open sourced database creation and distribution tool for [*Magic: The Gathering*](https://magic.wizards.com/en) cards, specifically in [JSON](https://json.org/) format.

This repo contains our newest release, version 4. This version relies upon a variety of sources, such as Scryfall and Gatherer for our data.


To provide feedback and/or bug reports, please open a ticket as it is the best way for us to communicate with the public.  

If you would like to join or assist the development of the project, you can [join us on Discord](https://discord.gg/Hgyg7GJ) to discuss things further.

# How To Use

This system was built using Python 3.7, so we can only guarentee proper functionality with this version.

1: To start, you'll need to install the MTGJSON4 package and dependencies. You can do this via:

```sh
python3 setup.py install
```

2: Select the flags you'd like to run the program with:

| Flag Options &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Flag Descriptions                                                                                                                                   	        |
|------------------------------	|-------------------------------------------------------------------------------------------------------------------------------------------------------------  |
| `-a`<br>`--all-sets`         	| This tells the program to build all sets. This supersedes the `-s` flag.                                                                                      |
| `-s SET1 SET2 ...`       	    | This tells the program to build all set codes passed, provided they exist.                                                                                	|
| `-f`<br>`--compiled-outputs` 	| This tells the program to compile AllCards and AllSets.                                                                                             	        |
| `-x`<br>`--skip-rebuild`     	| This tells the program to build no sets, and just used what is cached already. This supersedes the `-a` and `-s` flags, and is useful with the `-f` flag. 	|
| `-c`<br>`--skip-cached`      	| This flag, in conjunction with `-s` or `-a`, tells the program to skips sets that have already been built, and just used what is cached already.        	    |

3: Run the program via:
```sh
python3 mtgjson.py [-h] [-s SET [SET ...]] [-a] [-f] [-x] [-c]
```
