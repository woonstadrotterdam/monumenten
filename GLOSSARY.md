# Monumenten

Bepalen welke beschermde erfgoedstatussen gelden voor een BAG-verblijfsobject.

## Language

**Verblijfsobject**:
Het kleinste adresseerbare object uit de BAG waarvoor een monumentstatus wordt bepaald.
_Avoid_: Adres, pand, woning

**Provinciaal monument**:
Een object dat gedeputeerde staten op grond van de provinciale verordening als monument hebben aangewezen; dat komt alleen voor in Noord-Holland en Drenthe. Een voorbeschermd object (voornemen tot aanwijzing) is nog geen provinciaal monument.
_Avoid_: Provinciale bescherming, voorbeschermd monument

**Provinciaal beschermd dorpsgezicht**:
Een gebied dat een provincie als geheel beschermt om zijn cultuurhistorische samenhang; een verblijfsobject erin is daardoor géén **Provinciaal monument**. Alleen Barsingerhorn heeft deze status.
_Avoid_: Provinciaal monument (voor het gebied of de panden erin)

**Onbepaalde status**:
Een monumentstatus die `pd.NA` is: zij is niet gevonden, en een bron waar zij van afhangt gaf voor het verblijfsobject geen antwoord. Een gevonden status is nooit onbepaald.
_Avoid_: Onbekend, ontbrekend, False

**Onvolledig verblijfsobject**:
Een verblijfsobject waarvoor ten minste één bron geen antwoord gaf of waarvan de verwerking mislukte. Alleen de statussen die van die bron afhangen en niet gevonden zijn, zijn daardoor een **Onbepaalde status**; de rest is gewoon bepaald.
_Avoid_: Onbepaald verblijfsobject, mislukt verblijfsobject
