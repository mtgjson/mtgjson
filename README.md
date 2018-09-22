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

<table>
    <tr>
        <th style="width:40%">Flags</th>
        <th style="width:60%">Descriptions</th>
    </tr>
    <tr>
    <td><b>-a</b><br><b>--all-sets</b></td>
    <td>This tells the program to build all sets. This supersedes the <b>-s</b> flag.</td>
    </tr>
    <tr>
    <td><b>-s SET1 SET2 ...</b></td>
        <td>This tells the program to build all set codes passed, provided they exist.</td>
    </tr>
    <tr>
        <td><b>-f</b><br><b>--compiled-outputs</b></td>
        <td>This tells the program to compile AllCards and AllSets.</td>
    </tr>
    <tr>
        <td><b>-x</b><br><b>--skip-rebuild</b></td>
        <td>This tells the program to build no sets, and just used what is cached already. This supersedes the <b>-a</b> and <b>-s</b> flags, and is useful with the <b>-f</b> flag.</td>
    </tr>
    <tr>
        <td><b>-c</b><br><b>--skip-cached</b></td>
        <td>This flag, in conjunction with <b>-s</b> or <b>-a</b>, tells the program to skips sets that have already been built, and just used what is cached already.</td>
    </tr>
</table>
        

3: Run the program via:
```sh
python3 mtgjson.py [-h] [-s SET [SET ...]] [-a] [-f] [-x] [-c]
```
