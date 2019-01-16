"""Upstream data providers."""
from mtgjson4.providers.gamepedia import Gamepedia
from mtgjson4.providers.gatherer import Gatherer
from mtgjson4.providers.scryfall import Scryfall
from mtgjson4.providers.tcgplayer import TCGPlayer
from mtgjson4.providers.wizards import Wizards

GAMEPEDIA = Gamepedia()
GATHERER = Gatherer()
SCRYFALL = Scryfall()
TCGPLAYER = TCGPlayer()
WIZARDS = Wizards()
