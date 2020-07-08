# [MTGJSON v5](https://mtgjson.com/)
<p align="center"><a href="https://mtgjson.com/"><img src="https://www.mtgjson.com/images/assets/logo-mtgjson-dark-blue.svg" height="100px" alt="MTGJSON Logo"/></a></p>
<p align="center">
    <a href="https://mtgjson.com/changelog/"><img src="https://img.shields.io/badge/dynamic/json.svg?label=Version&url=https%3A%2F%2Fmtgjson.com%2Fapi%2Fv5%2FMeta.json&query=%24.data.version&colorB=blue" alt="MTGJSON Version"/></a> 
    <a href="https://mtgjson.com/changelog/"><img src="https://img.shields.io/badge/dynamic/json.svg?label=Release%20Date&url=https%3A%2F%2Fmtgjson.com%2Fapi%2Fv5%2FMeta.json&query=%24.data.date&colorB=blue" alt="MTGJSON Version Date"/></a>
    <br/><br/>
</p>


MTGJSON is an open-source repository of [Magic: The Gathering](https://magic.wizards.com/) card data, specifically in [JSON](https://json.org/) and [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) formats. This repository contains the build scripts we use to create our data sets.

## How to Contribute

Running a project as large as ours isn't easy, and we rely on the community to help keep our project going. The community can support us through two different ways: financial contributions and code contributions.

[![Github Sponsors](https://img.shields.io/static/v1.svg?label=GitHub%20Sponsors&message=Support%20MTGJSON&color=34d058&logo=github)](https://github.com/sponsors/ZeldaZach)  
We accept reoccurring donations via GitHub Sponsors, that grant priority support from MTGJSON maintainers and a special role on our Discord indicating your support.
**This is the preferred approach for financial contributions, as the MTGJSON team pays _no fees_ for these types of donations!**

[![Patreon](https://img.shields.io/static/v1.svg?label=Patreon&message=Support%20MTGJSON&color=f96854&logo=patreon)](https://patreon.com/mtgjson)  
We accept reoccurring donations via Patreon, that grant priority support from MTGJSON maintainers and a special role on our Discord indicating your support.  

[![PayPal](https://img.shields.io/static/v1.svg?label=PayPal&message=Support%20MTGJSON&color=009cde&logo=paypal)](https://paypal.me/zachhalpern)  
We accept one-time donations via PayPal, for those who want to say thank you to the project.

[![Code Contributions](https://img.shields.io/static/v1.svg?label=GitHub&message=Development&color=aaa&logo=github)](https://github.com/mtgjson)  
We love it when the community contributes back to the project! If you'd like to help improve our data for the hundreds of projects and stores we support, do reach out via [Discord](https://mtgjson.com/discord)!  

## Connect With Us  

[![Discord](https://img.shields.io/discord/224178957103136779?label=Discord&logo=discord&logoColor=white&color=7289da)](https://mtgjson.com/discord)  
The team stays in contact via [Discord](https://mtgjson.com/discord). The server is open to the public and is a great place to collaborate with other like-minded people. Stop by today and say hi!  

## About Us

### The Team  
The MTGJSON team has been led by Zach Halpern since 2018, with support from an awesome group of people. The full team lineup can be found [MTGJSON's homepage](https://mtgjson.com/).  

### Our Product
MTGJSON at its core is a database that can be downloaded for offline access to Magic: the Gathering card data. We pride ourselves on our [documentation](https://mtgjson.com/data-models/), and aim for full transparency with the community.  

### Our Partners  
Over time, MTGJSON has gone through a number of transitions to bring the best product for our consumers. We'd like to thank the following groups, in alphabetical order, for helping to support our mission by enriching our data:  
- [CardHoarder](https://www.cardhoarder.com/?affiliate_id=mtgjson&utm_source=mtgjson&utm_campaign=affiliate&utm_medium=card)
- [CardKingdom](https://www.cardkingdom.com/?partner=mtgjson&utm_source=mtgjson&utm_medium=affiliate&utm_campaign=mtgjson)
- [CardMarket](https://www.cardmarket.com/en/Magic?utm_campaign=card_prices&utm_medium=text&utm_source=mtgjson)
- [Gatherer](https://gatherer.wizards.com)
- [MTG.wtf](https://mtg.wtf/)
- [MTGBan](https://www.mtgban.com/)
- [Scryfall](https://scryfall.com)
- [TCGPlayer](https://www.tcgplayer.com/?partner=mtgjson&utm_campaign=affiliate&utm_medium=mtgjson&utm_source=mtgjson)
- [What's in Standard](https://whatsinstandard.com/)

## How to Use  
### For 99% of our Users  
MTGJSON supplies precompiled databases at https://mtgjson.com/api/v5/. **This is the recommended way to use our service**.  

As stated before, we pride ourselves on our documentation which can be found at https://mtgjson.com/. If you find anything to be unclear or ambiguous, please [open a ticket](https://github.com/mtgjson/mtgjson-website/issues) on our documentation repository so we can address your concern immediately.

We fully rebuild our API data once a week (on Monday afternoons) and our price dataset once a day. You can poll the [Meta.json](https://mtgjson.com/api/v5/Meta.json) file to see when our data was last updated.  

### For those who want to build MTGJSON locally  
Most of our users shouldn't have a need to build MTGJSON locally. However, there are always exceptions and we aren't ones to judge.  
#### Build Box
While MTGJSON will work on Windows, Mac, and Linux, we prefer working within the Linux environment for lower overheads and less manual dependency management.  

- For **Linux** based build boxes (we recommend Ubuntu 20.04), your build box should have at least 2 cores and 4 GiB of RAM available.  
- For **Mac** based build boxes, your build box should have at least 2 cores and 4 GiB of RAM available.
- For **Windows** based build boxes, your build box should have at least 4 cores and 8 GiB of RAM available.

#### Install Python3
MTGJSON is built on and tested against a wide range of Python3 verisons. Currently, we maintain support for the following versions:
- Python 3.6
- Python 3.7
- Python 3.8

#### Install MTGJSON
##### Local Installation
`python3 -m pip install /path/to/mtgjson5/`
##### PyPi Repository
We intend to put MTGJSON5 on the pip package archive in the near future, once the first set of revisions takes place.

#### Using MTGJSON
A fully up-to-date help menu can be achieved via `python3 -m mtgjson5 -h`, but for your convenience here is a recent rundown:  
```
usage: mtgjson5 [-h] [-s [SET [SET ...]] | -a] [-c] [-x] [-z] [-p]
                [-SS [SET [SET ...]]] [-PB] [-R] [-NA]

optional arguments:
  -h, --help            show this help message and exit
  -s [SET [SET ...]], --sets [SET [SET ...]]
                        Set(s) to build, using Scryfall set code notation.
                        Non-existent sets silently ignored.
  -a, --all-sets        Build all possible sets, overriding the --sets option.
  -c, --full-build      Build new prices, MTGSQLive, and compiled outputs like
                        AllPrintings.
  -x, --resume-build    While determining what sets to build, ignore
                        individual set files found in the output directory.
  -z, --compress        Compress the output folder's contents for
                        distribution.
  -p, --pretty          When dumping JSON files, prettify the contents instead
                        of minifying them.
  -SS [SET [SET ...]], --skip-sets [SET [SET ...]]
                        Purposely exclude sets from the build that may have
                        been set using --sets or --all-sets.

mtgjson maintainer arguments:
  -PB, --price-build    Build updated pricing data then exit.
  -R, --referrals       Create and maintain a referral map for referral
                        linkages.
  -NA, --no-alerts      Prevent push notifications from sending when property
                        keys are defined.
```

#### MTGJSON Environment Variables
Due to how the new system is built, a few advanced values can be set by the user in the shell environment.
- `MTGJSON5_DEBUG` When set to 1 or true, additional logging will be dumped to the output files
- `MTGJSON5_OUTPUT_PATH` When set, MTGJSON will dump all outputs to a specific directory
    - Ex:  `MTGJSON5_OUTPUT_PATH=~/Desktop` will dump database files to `/home/USER/Desktop/mtgjson_build_5XXX` and log files to `/home/USER/Desktop/logs`

## Licensing  
MTGJSON is a freely available product under the [MIT License](https://github.com/mtgjson/mtgjson/blob/master/LICENSE.txt), allowing our users to enjoy Magic: the Gathering data free of charge, in perpetuity.
