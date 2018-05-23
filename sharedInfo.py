def get_gatherer_sets():
    gatherer_sets = [
        #'Kaladesh',
        #'Dominaria',
        #'Aether Revolt',
        #'Amonkhet',
        #'Welcome Deck 2017',
        #'Hour of Devastation',
        'Ixalan'
        #'Rivals of Ixalan',
    ]

    return gatherer_sets


def get_super_types():
    super_types = [
        'Basic',
        'Legendary',
        'Snow',
        'World',
        'Ongoing'
    ]
    return super_types


def get_types():
    types = [
        'Instant',
        'Sorcery',
        'Artifact',
        'Creature',
        'Enchantment',
        'Land',
        'Planeswalker',
        'Tribal',
        'Plane',
        'Phenomenon',
        'Scheme',
        'Vanguard',
        'Conspiracy',
        'Host',

        # Un-Sets
        'Enchant',
        'Player',
        'Interrupt',
        'Scariest',
        'You\'ll',
        'Ever',
        'See',
        'Eaturecray'
    ]
    return types


def get_symbol_short_name(key_to_find):

    symbol_map = {
        'White': 'W',
        'Blue': 'U',
        'Black': 'B',
        'Red': 'R',
        'Green': 'G',
        'Snow': 'S',
        'Colorless': 'C',
        'Energy': 'E',
        'Variable Colorless': 'X',

        'Tap': 'T',
        'Untap': 'Q'
    }

    # Returns the key if not found
    return symbol_map.get(key_to_find, key_to_find)


"""
    x : [],
    y : [],
    z : [],
    wu : ['whiteblue', 'bluewhite', 'uw'],
    wb : ['whiteblack', 'blackwhite', 'bw'],
    ub : ['blueblack', 'blackblue', 'bu'],
    ur : ['bluered', 'redblue', 'ru'],
    br : ['blackred', 'redblack', 'rb'],
    bg : ['blackgreen', 'greenblack', 'gb'],
    rg : ['redgreen', 'greenred', 'gr'],
    rw : ['redwhite', 'whitered', 'wr'],
    gw : ['greenwhite', 'whitegreen', 'wg'],
    gu : ['greenblue', 'bluegreen', 'ug'],
    '2w' : ['twowhite', '2white', 'whitetwo', 'w2', 'white2'],
    '2u' : ['twoblue', '2blue', 'bluetwo', 'u2', 'blue2'],
    '2b' : ['twoblack', '2black', 'blacktwo', 'b2', 'black2'],
    '2r' : ['twored', '2red', 'redtwo', 'r2', 'red2'],
    '2g' : ['twogreen', '2green', 'greentwo', 'g2', 'green2'],
    p : ['phyrexian'],
    pw : ['phyrexianwhite', 'pwhite', 'whitephyrexian', 'whitep', 'wp', 'wphyrexian'],
    pu : ['phyrexianblue', 'pblue', 'bluephyrexian', 'bluep', 'up', 'uphyrexian'],
    pb : ['phyrexianblack', 'pblack', 'blackphyrexian', 'blackp', 'bp', 'bphyrexian'],
    pr : ['phyrexianred', 'pred', 'redphyrexian', 'redp', 'rp', 'rphyrexian'],
    pg : ['phyrexiangreen', 'pgreen', 'greenphyrexian', 'greenp', 'gp', 'gphyrexian'],
    '∞' : ['infinity'],
    h : ['half', 'halfcolorless', 'colorlesshalf'],
    hw : ['halfwhite', 'halfw', 'whitehalf', 'whalf', 'wh', 'whiteh'],
    hu : ['halfblue', 'halfu', 'bluehalf', 'uhalf', 'uh', 'blueh'],
    hb : ['halfblack', 'halfb', 'blackhalf', 'bhalf', 'bh', 'blackh'],
    hr : ['halfred', 'halfr', 'redhalf', 'rhalf', 'rh', 'redh'],
    hg : ['halfgreen', 'halfg', 'greenhalf', 'ghalf', 'gh', 'greenh']
    chaosdice : ['chaos', 'c'],
    };
"""