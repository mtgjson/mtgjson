"""
Generate TypeScript definitions from Pydantic models
"""
import pathlib
import sys
from typing import Any, Set, Union, get_args, get_origin

if __name__ == '__main__' and __package__ is None:
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from pydantic import BaseModel
from pydantic.fields import FieldInfo

# Import from your consolidated schema
print("Loading models from consolidated schema...")
try:
    from mtgjson5.models.schema import (
        # Leaf nodes
        MtgjsonIdentifiersObject,
        MtgjsonPurchaseUrlsObject,
        MtgjsonRelatedCardsObject,
        MtgjsonRulingsObject,
        MtgjsonLeadershipSkillsObject,
        MtgjsonLegalitiesObject,
        MtgjsonForeignDataObject,
        MtgjsonTranslationsObject,
        MtgjsonKeywordsObject,
        MtgjsonSourceProductsObject,
        MtgjsonTcgplayerSkusObject,
        MtgjsonMetaObject,
        # Prices
        MtgjsonPricePointsObject,
        MtgjsonPriceListObject,
        MtgjsonPriceFormatsObject,
        # Boosters
        MtgjsonBoosterPackObject,
        MtgjsonBoosterSheetObject,
        MtgjsonBoosterConfigObject,
        # Sealed Products
        MtgjsonSealedProductCardObject,
        MtgjsonSealedProductDeckObject,
        MtgjsonSealedProductOtherObject,
        MtgjsonSealedProductPackObject,
        MtgjsonSealedProductSealedObject,
        MtgjsonSealedProductContentsObject,
        MtgjsonSealedProductObject,
        # Card Types
        MtgjsonCardTypeObject,
        MtgjsonCardTypesObject,
        # Cards - all variants
        MtgjsonCardAtomicObject,
        MtgjsonCardDeckObject,
        MtgjsonCardSetDeckObject,
        MtgjsonCardSetObject,
        MtgjsonCardTokenObject,
        # Decks
        MtgjsonDeckObject,
        MtgjsonDeckListObject,
        MtgjsonDeckSetObject,
        # Sets
        MtgjsonSetObject,
        MtgjsonSetListObject,
    )
    print("✓ All models loaded successfully")
except ImportError as e:
    print(f"✗ Failed to import models: {e}")
    sys.exit(1)


class TypeScriptGenerator:
    """Generate TypeScript type definitions from Pydantic models"""
    
    def __init__(self):
        self.generated_types: Set[str] = set()
    
    def python_type_to_ts(self, field_info: FieldInfo, field_type: Any) -> str:
        """Convert Python type annotation to TypeScript type"""
        
        # Handle None type
        if field_type is type(None):
            return 'null'
        
        # Get the origin of the type (e.g., list, dict, Union)
        origin = get_origin(field_type)
        
        # Handle Union types (including Optional which is Union[X, None])
        if origin is Union:
            args = get_args(field_type)
            # Filter out None types
            non_none_args = [arg for arg in args if arg is not type(None)]
            
            if len(non_none_args) == 0:
                return 'null'
            elif len(non_none_args) == 1:
                # This is Optional[T] - just return the T type
                return self.python_type_to_ts(field_info, non_none_args[0])
            else:
                # Multiple non-None types in Union
                return ' | '.join(self.python_type_to_ts(field_info, arg) for arg in non_none_args)
        
        # Handle List types
        if origin is list:
            args = get_args(field_type)
            if args:
                inner_type = self.python_type_to_ts(field_info, args[0])
                return f'{inner_type}[]'
            return 'any[]'
        
        # Handle Dict types
        if origin is dict:
            args = get_args(field_type)
            if args and len(args) == 2:
                key_type = self.python_type_to_ts(field_info, args[0])
                val_type = self.python_type_to_ts(field_info, args[1])
                return f'Record<{key_type}, {val_type}>'
            return 'Record<string, any>'
        
        # Check if it's a Pydantic model
        try:
            if isinstance(field_type, type) and issubclass(field_type, BaseModel):
                # Map Python model names to TypeScript names
                name = field_type.__name__
                # Remove Mtgjson prefix and Object suffix
                name = name.replace('Mtgjson', '').replace('Object', '')
                # Special cases
                if name == 'Ruling':
                    return 'Rulings'
                return name
        except TypeError:
            pass
        
        # Get type name as string for basic types
        if hasattr(field_type, '__name__'):
            type_name = field_type.__name__
        else:
            type_name = str(field_type)
        
        # Map basic Python types to TypeScript
        basic_types = {
            'str': 'string',
            'int': 'number',
            'float': 'number',
            'bool': 'boolean',
            'Any': 'any',
            'NoneType': 'null',
        }
        
        result = basic_types.get(type_name, 'any')
        
        # Debug output for unmapped types
        if result == 'any' and type_name not in ['Any']:
            print(f"⚠ Warning: Unmapped type '{type_name}' for field, defaulting to 'any'")
        
        return result
    
    def generate_interface(self, model: type[BaseModel], name_override: str = None) -> str:
        """Generate TypeScript interface from Pydantic model"""
        if model is None:
            return ""
            
        interface_name = name_override or model.__name__.replace('Mtgjson', '').replace('Object', '')
        
        if interface_name in self.generated_types:
            return ""
        
        self.generated_types.add(interface_name)
        
        lines = [f'export type {interface_name} = {{']
        
        for field_name, field_info in model.model_fields.items():
            # Use alias if available (for camelCase output)
            ts_field_name = field_info.alias or field_name
            # Quote field names with spaces or special characters
            if ' ' in ts_field_name or '(' in ts_field_name or ')' in ts_field_name:
                ts_field_name = f'"{ts_field_name}"'
            # handle Optional| type|type fields
            is_optional = not field_info.is_required()
            optional_marker = '?' if is_optional else ''
            ts_type = self.python_type_to_ts(field_info, field_info.annotation)
            lines.append(f'  {ts_field_name}{optional_marker}: {ts_type};')
        
        lines.append('};')
        
        return '\n'.join(lines)
    
    def generate_all_types(self) -> str:
        """Generate all MTGJSON TypeScript types"""
        output = [
            '// Auto-generated TypeScript definitions for MTGJSON',
            '// DO NOT EDIT - Generated from Python Pydantic models',
            '// To regenerate: python -m mtgjson5.types_generator',
            '',
        ]
        
        # Generate in logical order
        
        # 1. Leaf nodes / shared objects
        output.append('// Shared objects')
        for model, name in [
            (MtgjsonIdentifiersObject, 'Identifiers'),
            (MtgjsonPurchaseUrlsObject, 'PurchaseUrls'),
            (MtgjsonRelatedCardsObject, 'RelatedCards'),
            (MtgjsonRulingsObject, 'Rulings'),
            (MtgjsonLeadershipSkillsObject, 'LeadershipSkills'),
            (MtgjsonLegalitiesObject, 'Legalities'),
            (MtgjsonForeignDataObject, 'ForeignData'),
            (MtgjsonTranslationsObject, 'Translations'),
            (MtgjsonKeywordsObject, 'Keywords'),
            (MtgjsonSourceProductsObject, 'SourceProducts'),
            (MtgjsonTcgplayerSkusObject, 'TcgplayerSkus'),
            (MtgjsonMetaObject, 'Meta'),
        ]:
            interface = self.generate_interface(model, name)
            if interface:
                output.append(interface)
                output.append('')
        
        # 2. Price types
        output.append('// Price types')
        for model, name in [
            (MtgjsonPricePointsObject, 'PricePoints'),
            (MtgjsonPriceListObject, 'PriceList'),
            (MtgjsonPriceFormatsObject, 'PriceFormats'),
        ]:
            interface = self.generate_interface(model, name)
            if interface:
                output.append(interface)
                output.append('')
        
        # 3. Booster types
        output.append('// Booster types')
        for model, name in [
            (MtgjsonBoosterPackObject, 'BoosterPack'),
            (MtgjsonBoosterSheetObject, 'BoosterSheet'),
            (MtgjsonBoosterConfigObject, 'BoosterConfig'),
        ]:
            interface = self.generate_interface(model, name)
            if interface:
                output.append(interface)
                output.append('')
        
        # 4. Sealed product types
        output.append('// Sealed product types')
        for model, name in [
            (MtgjsonSealedProductCardObject, 'SealedProductCard'),
            (MtgjsonSealedProductDeckObject, 'SealedProductDeck'),
            (MtgjsonSealedProductOtherObject, 'SealedProductOther'),
            (MtgjsonSealedProductPackObject, 'SealedProductPack'),
            (MtgjsonSealedProductSealedObject, 'SealedProductSealed'),
            (MtgjsonSealedProductContentsObject, 'SealedProductContents'),
            (MtgjsonSealedProductObject, 'SealedProduct'),
        ]:
            interface = self.generate_interface(model, name)
            if interface:
                output.append(interface)
                output.append('')
        
        # 5. Card type structure
        output.append('// Card type structure')
        for model, name in [
            (MtgjsonCardTypeObject, 'CardType'),
            (MtgjsonCardTypesObject, 'CardTypes'),
        ]:
            interface = self.generate_interface(model, name)
            if interface:
                output.append(interface)
                output.append('')
        
        # 6. Card variants
        output.append('// Card variants')
        for model, name in [
            (MtgjsonCardAtomicObject, 'CardAtomic'),
            (MtgjsonCardDeckObject, 'CardDeck'),
            (MtgjsonCardSetDeckObject, 'CardSetDeck'),
            (MtgjsonCardSetObject, 'CardSet'),
            (MtgjsonCardTokenObject, 'CardToken'),
        ]:
            interface = self.generate_interface(model, name)
            if interface:
                output.append(interface)
                output.append('')
        
        # 7. Deck types
        output.append('// Deck types')
        for model, name in [
            (MtgjsonDeckObject, 'Deck'),
            (MtgjsonDeckListObject, 'DeckList'),
            (MtgjsonDeckSetObject, 'DeckSet'),
        ]:
            interface = self.generate_interface(model, name)
            if interface:
                output.append(interface)
                output.append('')
        
        # 8. Set types
        output.append('// Set types')
        for model, name in [
            (MtgjsonSetObject, 'Set'),
            (MtgjsonSetListObject, 'SetList'),
        ]:
            interface = self.generate_interface(model, name)
            if interface:
                output.append(interface)
                output.append('')
        
        # 9. File format wrappers
        output.append('// File format wrappers')
        file_wrappers = [
            'export type AllPrintingsFile = { meta: Meta; data: Record<string, Set>; };',
            'export type AllPricesFile = { meta: Meta; data: Record<string, PriceFormats>; };',
            'export type AllIdentifiersFile = { meta: Meta; data: Record<string, CardSet>; };',
            'export type AtomicCardsFile = { meta: Meta; data: Record<string, CardAtomic>; };',
            'export type EnumValuesFile = { meta: Meta; data: Record<string, Record<string, string[]>>; };',
            'export type CompiledListFile = { meta: Meta; data: string[]; };',
            'export type LegacyFile = { meta: Meta; data: Record<string, CardSet>; };',
            'export type LegacyAtomicFile = { meta: Meta; data: Record<string, CardAtomic>; };',
            'export type ModernFile = { meta: Meta; data: Record<string, CardSet>; };',
            'export type ModernAtomicFile = { meta: Meta; data: Record<string, CardAtomic>; };',
            'export type PauperAtomicFile = { meta: Meta; data: Record<string, CardAtomic>; };',
            'export type PioneerFile = { meta: Meta; data: Record<string, CardSet>; };',
            'export type PioneerAtomicFile = { meta: Meta; data: Record<string, CardAtomic>; };',
            'export type StandardFile = { meta: Meta; data: Record<string, CardSet>; };',
            'export type StandardAtomicFile = { meta: Meta; data: Record<string, CardAtomic>; };',
            'export type VintageFile = { meta: Meta; data: Record<string, CardSet>; };',
            'export type VintageAtomicFile = { meta: Meta; data: Record<string, CardAtomic>; };',
        ]
        output.extend(file_wrappers)
        
        return '\n'.join(output)


def generate_types_file(output_path: pathlib.Path) -> None:
    """Generate types.ts file from Pydantic models"""
    generator = TypeScriptGenerator()
    typescript_code = generator.generate_all_types()
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(typescript_code, encoding='utf-8')
    print(f"\n✅ Generated TypeScript definitions: {output_path}")
    print(f"   Generated {len(generator.generated_types)} unique types")
    print(f"   Output size: {len(typescript_code):,} characters")


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate TypeScript types from Pydantic models')
    parser.add_argument(
        '--output',
        type=pathlib.Path,
        default=pathlib.Path('./types/types.ts'),
        help='Output file path (default: ./types/types.ts)'
    )
    
    args = parser.parse_args()
    generate_types_file(args.output)


if __name__ == '__main__':
    main()