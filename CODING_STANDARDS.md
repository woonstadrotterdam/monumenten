# Codeerstandaarden

Afwegingen die geen linter kan maken. Opmaak, types en docstrings bewaakt pre-commit.

## README

- De README is voor gebruikers van de package en beschrijft de code zoals die nu is. Waarom iets veranderde, metingen en vergelijkingen met de oude situatie horen in de PR-beschrijving.
- Een afkorting of bronnaam (BGT, BRK, PDOK, EWE) staat bij het eerste gebruik voluit, met in een paar woorden wat het is.

## Tests

- Elke test bewaakt na de merge nog gedrag dat kan terugvallen. Een test die alleen aantoont dat deze PR werkt, zoals een eenmalige meting of een bestaande test met andere waarden, hoort in de PR-beschrijving.
