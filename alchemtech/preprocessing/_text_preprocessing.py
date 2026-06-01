import unicodedata


def remove_accents(input_str: str) -> str:
    """
    Supprime les accents d'une chaîne de caractères en utilisant la normalisation Unicode.

    Exemple :
        "Éléphant à l'école" → "Elephant a l'ecole"

    Parameters
    ----------
    input_str : str
        Chaîne de caractères contenant potentiellement des caractères accentués.

    Returns
    -------
    str
        Chaîne sans accents.
    """
    nfkd_form = unicodedata.normalize("NFKD", input_str)
    return "".join(c for c in nfkd_form if not unicodedata.combining(c))