from typing import Any, Dict

import pytest

from mtgjson4.mtgjson_card import MTGJSONCard


@pytest.mark.parametrize(
    "mock_card, mock_file_info",
    [
        (
            {
                "artist": "Clint Cearley",
                "borderColor": "black",
                "colorIdentity": ["W"],
                "colors": ["W"],
                "convertedManaCost": 4.0,
                "edhrecRank": 1415,
                "flavorText": "She now hears only Emrakul's murmurs.",
                "foreignData": [
                    {
                        "flavorText": "Nun hört sie nur noch Emrakuls Flüstern.",
                        "language": "German",
                        "multiverseId": 414988,
                        "name": "Sela, das gebrochene Schwert",
                        "text": "Fliegend, Erstschlag, Lebensverknüpfung\nZu Beginn deines Endsegments und falls du Sela, das gebrochene Schwert, und eine Kreatur namens Bruna, das schwindende Licht, besitzt und kontrollierst, schicke beide ins Exil und verschmilz sie dann zu Brisela, Stimme der Albträume.",
                        "type": "Legendäre Kreatur — Engel, Schrecken",
                    },
                    {
                        "flavorText": "Ahora solo escucha los susurros de Emrakul.",
                        "language": "Spanish",
                        "multiverseId": 416549,
                        "name": "Gisela, la Espada Quebrada",
                        "text": "Vuela, daña primero, vínculo vital.\nAl comienzo de tu paso final, si eres propietario de Gisela, la Espada Quebrada y de Bruna, la Luz Mortecina y controlas ambas cartas, exílialas. Luego, combínalas para formar Brisela, Voz de las Pesadillas.",
                        "type": "Criatura legendaria — Horror ángel",
                    },
                    {
                        "flavorText": "Elle n'entend désormais plus que les murmures d'Emrakul.",
                        "language": "French",
                        "multiverseId": 415211,
                        "name": "Gisela, la lame brisée",
                        "text": "Vol, initiative, lien de vie\nAu début de votre étape de fin, si vous possédez et contrôlez à la fois Gisela, la lame brisée et une créature appelée Bruna, la lumière pâlissante, exilez-les, puis assimilez-les en Brisela, voix des cauchemars.",
                        "type": "Créature légendaire : ange et horreur",
                    },
                    {
                        "flavorText": "Ormai avverte solo i sussurri di Emrakul.",
                        "language": "Italian",
                        "multiverseId": 415434,
                        "name": "Gisela, Lama Spezzata",
                        "text": "Volare, attacco improvviso, legame vitale\nAll'inizio della tua sottofase finale, se possiedi e controlli Gisela, Lama Spezzata e una creatura chiamata Bruna, Luce Morente, esiliale, poi combinale in Brisela, Voce degli Incubi.",
                        "type": "Creatura Leggendaria — Orrore Angelo",
                    },
                    {
                        "flavorText": "今や彼女の耳にはエムラクールの呟きしか聞こえていない。",
                        "language": "Japanese",
                        "multiverseId": 415657,
                        "name": "折れた刃、ギセラ",
                        "text": "飛行、先制攻撃、絆魂\nあなたの終了ステップの開始時に、あなたが折れた刃、ギセラと「消えゆく光、ブルーナ」という名前のクリーチャーを１体コントロールしているとともにそれらのオーナーである場合、それらを追放し、その後それらを悪夢の声、ブリセラへと合体させる。",
                        "type": "伝説のクリーチャー — 天使・ホラー",
                    },
                    {
                        "flavorText": "이제 그녀에게는 엠라쿨의 속삭임만이 들린다.",
                        "language": "Korean",
                        "multiverseId": 415880,
                        "name": "부러진 검 기셀라",
                        "text": "비행, 선제공격, 생명연결\n당신의 종료단 시작에, 당신이 부러진 검 기셀라와 이름이 퇴색하는 빛 브루나인 생물을 모두 소유하고 조종한다면, 그 둘을 추방한 다음 그 둘을 악몽의 목소리 브리셀라로 혼합한다.",
                        "type": "전설적 생물 — 천사 괴수",
                    },
                    {
                        "flavorText": "Agora, ela ouve apenas os murmúrios de Emrakul.",
                        "language": "Portuguese (Brazil)",
                        "multiverseId": 416103,
                        "name": "Gisela, a Lâmina Partida",
                        "text": "Voar, iniciativa, vínculo com a vida\nNo início de sua etapa final, se você for o dono e controlar Gisela, a Lâmina Partida, e uma criatura com o nome Bruna, a Luz Desvanecente, exile-as e, em seguida, funda-as em Brisela, Voz dos Pesadelos.",
                        "type": "Criatura Lendária — Anjo Horror",
                    },
                    {
                        "flavorText": "Теперь она слышит лишь шепот Эмракул.",
                        "language": "Russian",
                        "multiverseId": 416326,
                        "name": "Гизела, Сломанный Клинок",
                        "text": "Полет, Первый удар, Цепь жизни\nВ начале вашего заключительного шага, если вы одновременно являетесь владельцем и контролируете Гизелу, Сломанный Клинок и существо с именем Бруна, Затухающий Светоч, изгоните их, затем соедините их в Бризелу, Голос Кошмаров.",
                        "type": "Легендарное Существо — Ангел Ужас",
                    },
                    {
                        "flavorText": "她双耳只闻伊莫库口中幽声。",
                        "language": "Chinese Simplified",
                        "multiverseId": 414542,
                        "name": "破碎之刃姬瑟拉",
                        "text": "飞行，先攻，系命\n在你的结束步骤开始时，若由你拥有且由你操控的永久物中同时有破碎之刃姬瑟拉与一个名称为渐逝之光布鲁娜的生物，则放逐它们，然后将它们融合为梦魇异音布瑟拉。",
                        "type": "传奇生物～天使／惊惧兽",
                    },
                    {
                        "flavorText": "她雙耳只聞伊莫庫口中幽聲。",
                        "language": "Chinese Traditional",
                        "multiverseId": 414765,
                        "name": "破碎之刃姬瑟拉",
                        "text": "飛行，先攻，繫命\n在你的結束步驟開始時，若由你擁有且由你操控的永久物中同時有破碎之刃姬瑟拉與一個名稱為漸逝之光布魯娜的生物，則放逐它們，然後將它們融合為夢魘異音布瑟拉。",
                        "type": "傳奇生物～天使／驚懼獸",
                    },
                ],
                "frameVersion": "2015",
                "hasFoil": True,
                "hasNonFoil": True,
                "isMtgo": True,
                "isPaper": True,
                "layout": "meld",
                "legalities": {
                    "commander": "Legal",
                    "duel": "Legal",
                    "legacy": "Legal",
                    "modern": "Legal",
                    "pioneer": "Legal",
                    "vintage": "Legal",
                },
                "manaCost": "{2}{W}{W}",
                "mtgoId": 61100,
                "multiverseId": 414319,
                "name": "Gisela, the Broken Blade",
                "names": [
                    "Bruna, the Fading Light",
                    "Brisela, Voice of Nightmares",
                    "Gisela, the Broken Blade",
                ],
                "number": "28",
                "originalText": "Flying, first strike, lifelink\nAt the beginning of your end step, if you both own and control Gisela, the Broken Blade and a creature named Bruna, the Fading Light, exile them, then meld them into Brisela, Voice of Nightmares.",
                "originalType": "Legendary Creature — Angel Horror",
                "otherFaceIds": [],
                "power": "4",
                "prices": {},
                "printings": ["EMN", "PEMN", "V17"],
                "purchaseUrls": {},
                "rarity": "mythic",
                "rulings": [
                    {
                        "date": "2016-07-13",
                        "text": "In a Commander game, your commander may be Bruna, the Fading Light or Gisela, the Broken Blade, and the other may be in your deck. If they meld into Brisela, Voice of Nightmares, Brisela will also be your commander; but if Brisela leaves the battlefield, only the card chosen as your commander at the start of the game may be put into the command zone.",
                    },
                    {
                        "date": "2016-07-13",
                        "text": "Effects that increase or reduce the cost to cast a spell (such as those of escalate and emerge) don’t affect the spell’s converted mana cost, so they won’t change whether Brisela’s last ability restricts that spell from being cast.",
                    },
                    {
                        "date": "2016-07-13",
                        "text": "For spells with {X} in their mana costs, use the value chosen for X to determine if the spell’s converted mana cost is 3 or less. For example, your opponent could cast Burn from Within (a spell with mana cost {X}{R}) with X equal to 3, but not with X equal to 2.",
                    },
                    {
                        "date": "2016-07-13",
                        "text": "For more information on meld cards, see the Eldritch Moon mechanics article (https://magic.wizards.com/en/articles/archive/feature/eldritch-moon-mechanics-2016-06-27).",
                    },
                ],
                "scryfallId": "c75c035a-7da9-4b36-982d-fca8220b1797",
                "scryfallIllustrationId": "db5289ab-8aa4-412d-afd4-f7b7fef475fb",
                "scryfallOracleId": "f3e23d5e-bd88-4e7c-a3fb-db2a8cb05b22",
                "side": "b",
                "subtypes": ["Angel", "Horror"],
                "supertypes": ["Legendary"],
                "tcgplayerProductId": 119687,
                "text": "Flying, first strike, lifelink\nAt the beginning of your end step, if you both own and control Gisela, the Broken Blade and a creature named Bruna, the Fading Light, exile them, then meld them into Brisela, Voice of Nightmares.",
                "toughness": "3",
                "type": "Legendary Creature — Angel Horror",
                "types": ["Creature"],
                "uuid": "4b560297-2f1e-5f65-b118-289c21bdf887",
            },
            {"code": "EMN"},
        )
    ],
)
def test_uuid_creation(
    mock_card: Dict[str, Any], mock_file_info: Dict[str, Any]
) -> None:
    """
    Tests to ensure UUIDs don't regress
    :param mock_card:
    :param mock_file_info:
    :return:
    """
    card = MTGJSONCard(mock_file_info["code"])
    card.set_all(mock_card)

    uuid_new = card.get_uuid()
    assert uuid_new == "4b560297-2f1e-5f65-b118-289c21bdf887"
