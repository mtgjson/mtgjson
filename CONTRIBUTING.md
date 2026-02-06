# Contributing to MTGJSON

First off, thanks for taking the time to read this document! It means something to us that you're potentially interested in helping improve our project.

Our goal for MTGJSON was and is to provide free and easily accessible Magic: the Gathering data for those who want and/or need it.

If you're not a software developer, but want to help the project in some way, we are always looking for individuals to audit the content we provide. We also accept financial contributions to the project via [PayPal](https://www.paypal.me/zachhalpern) and [Patreon](https://patreon.com/mtgjson).

## Environment Setup

Here at MTGJSON, we recommend all of our developers check out [PyCharm](https://jetbrains.com/pycharm/). While any IDE or notepad can be used, we found that the JetBrains environment is intuitive and easy to understand.

In addition to PyCharm, you'll need to install Python 3. We develop actively on the latest release of Python, but aim for compatibility with all supported 3.x versions.

- Mac: `brew install python3`
- Linux: `sudo apt-get install python3.11`
- Windows: [Download Python 3](https://www.python.org/downloads/)

### Setup with uv

We use [uv](https://docs.astral.sh/uv/) for dependency management. To get started:

- `uv sync --extra dev`
- `cp mtgjson.properties.example mtgjson5/resources/mtgjson.properties`
- Fill in your credentials into `mtgjson5/resources/mtgjson.properties`
  - Missing credentials won't fail the run, just extend the generated data
- `uv run python -m mtgjson5`

## Project Hierarchy

The codebase is split up into different files based on functionality. While not perfect, we aim to segregate components based on their usages and dependencies.

- `provider/`  is where we put code for each 3rd party resource we need to contact in order to collate our data. Each source should be given its own folder and should not be dependent on any other provider.
- `resources/` is where we put MTGJSON corrections, fixes, and hard caches. These are data points we want to include, but can't grab from an external entity easily or without causing a circular loop.
- `__main__.py` is where we define our globals for the project. TODO is to re-write them into their own class.
- `compile_mtg.py` is the main functionality of the project, where we handle turning data blobs into MTGJSON cards and sets.
- `compressor.py` is where we compress completed outputs for release.
- `outputter.py` is where all I/O operations should take place. All write to disk calls should be done from here.
- `util.py` are common utilities that may be used across the codebase.

## Code Style & Testing

We follow the [black](https://pypi.org/project/black/) style guides, a stricter version of [PEP-8](https://www.python.org/dev/peps/pep-0008/) styling.

To reformat your code and ensure compatibility with our system, run tox before you open up a pull request.

- Ensure dev dependencies are installed: `uv sync --extra dev`
- Run tox: `uv run tox`
- In PyCharm, you can also create a **Run > Edit Configurations > + > tox** run configuration

You will need to pass all Tox requirements, including the unit tests, for your pull request to be approved.

## Communication Channels

We communicate almost exclusively through [Discord](https://mtgjson.com/discord). Feel free to join us, whether you're a developer or not. We love chatting with the community!

MTGJSON staff can be identified with a blue name, and our staff listing is found on the [website](https://mtgjson.com/).
