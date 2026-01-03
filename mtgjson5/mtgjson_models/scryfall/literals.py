"""
Literal types for Scryfall models.
"""

from typing import Literal


Color = Literal["W", "U", "B", "R", "G"]
ManaColor = Literal["W", "U", "B", "R", "G", "C"]
Rarity = Literal["common", "uncommon", "rare", "mythic", "special", "bonus"]
BorderColor = Literal["black", "white", "borderless", "silver", "gold"]
Frame = Literal["1993", "1997", "2003", "2015", "future"]
SecurityStamp = Literal["oval", "triangle", "acorn", "circle", "arena", "heart"]
ImageStatus = Literal["missing", "placeholder", "lowres", "highres_scan"]
LegalityStatus = Literal["legal", "not_legal", "restricted", "banned"]
Finish = Literal["foil", "nonfoil", "etched", "glossy"]
Game = Literal["paper", "arena", "mtgo", "astral", "sega"]
Component = Literal["token", "meld_part", "meld_result", "combo_piece"]

Layout = Literal[
	"normal",
	"split",
	"flip",
	"transform",
	"modal_dfc",
	"meld",
	"leveler",
	"class",
	"case",
	"saga",
	"adventure",
	"mutate",
	"prototype",
	"battle",
	"planar",
	"scheme",
	"vanguard",
	"token",
	"double_faced_token",
	"emblem",
	"augment",
	"host",
	"art_series",
	"reversible_card",
]

FrameEffect = Literal[
	"legendary",
	"miracle",
	"enchantment",
	"draft",
	"devoid",
	"tombstone",
	"colorshifted",
	"inverted",
	"sunmoondfc",
	"compasslanddfc",
	"originpwdfc",
	"mooneldrazidfc",
	"waxingandwaningmoondfc",
	"showcase",
	"extendedart",
	"companion",
	"etched",
	"snow",
	"lesson",
	"shatteredglass",
	"convertdfc",
	"fandfc",
	"upsidedowndfc",
	"spree",
]
