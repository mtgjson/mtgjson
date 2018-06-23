[**MTG JSON**](https://mtgjson.com/) is a database of [*Magic: The Gathering*](https://magic.wizards.com/en) cards in [JSON](https://json.org/) format. This is the code for the next major release, which will be version 4.

If you would like to talk to the devs, [join us on Discord](https://discord.gg/Hgyg7GJ)!

# How to build

First, you will need at least Python 3.6.

Then, run the following to install dependencies:

```sh
python3 setup.py install
```

To build all set files, as well as `AllCards.json`, `AllSets.json`, and `AllSetsArray.json`, run:

```sh
python3 -m mtgjson4 -af
```

For advanced options, see `python3 -m mtgjson4 --help`.
