import pandas as pd
import numpy as np
import unicodedata
from pathlib import Path
import datetime

import os
import glob

import re


def retrieve_declaration_id(
        dataframe: pd.DataFrame,
        col_photo_name: str,
        return_dataframe: bool = False
    ):

    """
    Extrait un identifiant de déclaration ChassAdapt à partir d'une colonne contenant des noms de photos.

    L'identifiant extrait correspond aux 5 premiers segments séparés par "-", 
    par exemple :
        "4d1dc874-b8c8-4a13-8ccb-2d992b7b2e41-0-AileDroite-1.png" → "4d1dc874-b8c8-4a13-8ccb-2d992b7b2e41"

    Parameters
    ----------
    dataframe : pandas.DataFrame
        Le dataframe contenant la colonne de noms de photos.
    col_photo_name : str
        Nom de la colonne contenant les noms de fichiers (ex: "Photo_Name").
    return_dataframe : bool, optional (default=False)
        - True : renvoie le dataframe original avec une nouvelle colonne "Declaration_Id_Extracted"
        - False : renvoie seulement une liste des identifiants extraits

    Returns
    -------
    pandas.DataFrame or list[str]
        Selon le paramètre return_dataframe.
    """

    # Copie pour éviter de modifier le dataframe d’origine
    df = dataframe.copy()

    # Vérification que la colonne existe
    if col_photo_name not in df.columns:
        raise ValueError(f"La colonne '{col_photo_name}' est absente du dataframe.")

    # Extraction de l'ID
    df["Declaration_Id_Extracted"] = (
        df[col_photo_name]
        .astype(str)
        .str.split("-")
        .str[:5]
        .str.join("-")
    )

    # Renvoi selon l’option choisie
    if return_dataframe:
        return df
    return df["Declaration_Id_Extracted"].to_list()


def retrieve_bird_id(
        dataframe: pd.DataFrame,
        col_photo_name: str,
        return_dataframe: bool = False
    ):

    """
    Extrait un identifiant d'oiseau ("Bird ID") à partir d'une colonne contenant des noms de photos.

    L'identifiant extrait correspond aux 6 premiers segments séparés par "-",
    par exemple :
        "4d1dc874-b8c8-4a13-8ccb-2d992b7b2e41-0-AileDroite-1.png" → "4d1dc874-b8c8-4a13-8ccb-2d992b7b2e41-0"

    Parameters
    ----------
    dataframe : pandas.DataFrame
        Le dataframe contenant la colonne de noms de photos.
    col_photo_name : str
        Nom de la colonne contenant les noms de fichiers (ex : "Photo_Name").
    return_dataframe : bool, optional (default=False)
        - True : retourne le dataframe original avec une nouvelle colonne 'Bird_Id_Extracted'
        - False : retourne seulement la liste des identifiants extraits

    Returns
    -------
    pandas.DataFrame or list[str]
        Selon le paramètre return_dataframe.
    """

    # Copie pour éviter de modifier le dataframe initial
    df = dataframe.copy()

    # Vérification de l'existence de la colonne
    if col_photo_name not in df.columns:
        raise ValueError(f"La colonne '{col_photo_name}' est absente du dataframe.")

    # Extraction du Bird ID (6 segments)
    df["Bird_Id_Extracted"] = (
        df[col_photo_name]
        .astype(str)
        .str.split("-")
        .str[:6]
        .str.join("-")
    )

    # Renvoi selon l'option
    if return_dataframe:
        return df
    return df["Bird_Id_Extracted"].to_list()


class DataframeProcessing():
  def __init__(self,
               dataframe: pd.DataFrame):
    self.dataframe = dataframe.copy()
    self.history = []

  def _remove_accents(self, s):
    nfkd_form = unicodedata.normalize('NFKD', s)
    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

  def _strip(self, s: str) -> str:
    return s.strip()

  def _lower(self, s: str) -> str:
    return s.lower()

  def _special_case(self, s: str) -> str:
    return re.sub(r"[^\w\s]", "", s)

  def _underscore(self, s: str) -> str:
    return re.sub(r"\s+", "_", s)

  def columns_names_processing(self) -> pd.DataFrame:
    old_columns = list(self.dataframe.columns)
    new_columns = []

    for col in old_columns:
      original_col = col
      col = self._remove_accents(col)
      col = self._strip(col)
      col = self._lower(col)
      col = self._special_case(col)
      col = self._underscore(col)

      new_columns.append(col)
      self.history.append((original_col, col))

    self.dataframe.columns = new_columns
    return self.dataframe

  def date_processing(self,
                      date_col: str,
                      dayfirst: bool=False,
                      yearfirst: bool=True,
                      create_date_time: bool=False) -> pd.DataFrame:
    self.date_col = date_col
    self.dayfirst = dayfirst
    self.yearfirst = yearfirst
    self.create_date_time = create_date_time
    self.dataframe[self.date_col] = pd.to_datetime(self.dataframe[self.date_col],
                                                   dayfirst=self.dayfirst,
                                                   yearfirst=self.yearfirst,
                                                   errors="coerce")

    if self.create_date_time:
      new_date_col = "date" if "date" not in self.dataframe.columns else "date_new"
      self.new_date_col = new_date_col
      self.dataframe[new_date_col] = self.dataframe[self.date_col].dt.date
      self.dataframe["heure"] = self.dataframe[self.date_col].dt.time
    return self.dataframe


  def show_columns_names_changes(self):
    print("📝 Historique des modifications de colonnes :")
    for old, new in self.history:
      if old != new:
        print(f"  - '{old}' → '{new}'")
        
        
class FNCDataframeProcessing(DataframeProcessing):
  def __init__(self,
               dataframe: pd.DataFrame,
               ):
    super().__init__(dataframe)
    self.dataframe       = dataframe.copy()
    self.history         = []


  def seasons_processing(self, date_col: str, create_date_time: bool=True) -> pd.DataFrame:
    self.dataframe = super().date_processing(date_col, create_date_time=create_date_time)
    month = self.dataframe[self.date_col].dt.month
    year = self.dataframe[self.date_col].dt.year.astype("Int64")
    base_year = year.where(~month.isin([1, 2]), year - 1)
    saison = (base_year.astype(str) + "-" + (base_year + 1).astype(str))

    self.dataframe["mois"] = np.where(self.dataframe[self.new_date_col].notna(), month, "Indéterminé")
    self.dataframe["annee"] = np.where(self.dataframe[self.new_date_col].notna(), year, "Indéterminé")
    self.dataframe["saison"] = np.where(self.dataframe[self.new_date_col].notna(), saison, "Indéterminé")
    return self.dataframe


  def federations_processing(self, federations_col: str) -> pd.DataFrame:
    self.federations_col = federations_col
    if self.federations_col != "federation":
        self.dataframe.rename(columns={self.federations_col: "federation"}, inplace=True)
        self.federations_col = "federation"

    self.dataframe[self.federations_col] = self.dataframe[self.federations_col].astype(str).str.lstrip("0").str.zfill(2)
    return self.dataframe
    
    
class ChassAdaptDataframeProcessing(FNCDataframeProcessing):
  def __init__(self,
               dataframe: pd.DataFrame,
               ):
    super().__init__(dataframe)
    self.dataframe       = dataframe.copy()

  def species_processing(self, species_col: str) -> pd.DataFrame:
    self.species_col = species_col
    if self.species_col != "espece_declaree":
        self.dataframe.rename(columns={self.species_col: "espece_declaree"}, inplace=True)
        self.species_col = "espece_declaree"
    self.dataframe[self.species_col] = self.dataframe[self.species_col].astype(str).str.replace("(Fuligule)", "").str.replace("Fuligule", "").str.replace("(canard)", "").str.strip().str.capitalize()
    return self.dataframe

  def remove_test_processing(self, nom_col: str) -> pd.DataFrame:
    self.nom_col = nom_col
    self.dataframe = self.dataframe[~self.dataframe[self.nom_col].astype(str).str.contains("[Tt][Ee][Ss][Tt][12]")]
    return self.dataframe


class DataFromISIGEO():
  def __init__(self, dataframe: pd.DataFrame):
    self.dataframe = dataframe.copy()

  def rename_columns(self, date_col, espece_col, federation_col, photo_dessus_col, photo_dessous_col, photo_dessus_a_garder_col, sexe_col, age_col,
                     comment_col, nom_expert_col, heure_col, mois_col, annee_col, saison_col,
                     commentaire_libre_col=None, photo_dessous_a_garder_col=None):
    rename_map = {
                      date_col: "date_declaration",
                      espece_col: "espece",
                      federation_col: "federation",
                      photo_dessus_col: "photo_dessus",
                      photo_dessous_col: "photo_dessous",
                      photo_dessus_a_garder_col: "photo_dessus_a_garder",
                      sexe_col: "sexe",
                      age_col: "age",
                      comment_col: "comment",
                      nom_expert_col: "nom_expert",
                      heure_col: "heure",
                      mois_col: "mois",
                      annee_col: "annee",
                      saison_col: "saison",
                  }
    rename_map.update({col: new_name for col, new_name in [(photo_dessous_a_garder_col, "photo_dessous_a_garder"),
                                                           (commentaire_libre_col, "commentaire_libre")]
                          if col is not None
                      })
    self.dataframe.rename(columns=rename_map, inplace=True)
    for new_name in rename_map.values():
        setattr(self, new_name + "_col", new_name)

  def process_proba_column(self, proba_col: str, round_value: int=1):
    self.round_value = round_value
    self.dataframe[proba_col] = np.where(self.dataframe[proba_col] < 1.0,
                                         self.dataframe[proba_col] * 100,
                                         self.dataframe[proba_col]).round(self.round_value)

  def select_validated_annotations(self):
    self.dataframe_validated = self.dataframe[~(self.dataframe[self.photo_dessus_a_garder_col] == "Non") & ~(self.dataframe[self.age_col].isna())].reset_index(drop=True)
    if hasattr(self, "photo_dessous_a_garder_col"):
      self.dataframe_validated = self.dataframe_validated[~(self.dataframe_validated[self.photo_dessous_a_garder_col] == "Non")]

  def process_sexe_age_columns(self, col_source: str, col_output: str):
    self.dataframe_validated = self._process_sexe_age(self.dataframe_validated, col_source, col_output)
    setattr(self, col_output + "_col", col_output)
    return self.dataframe_validated

  def process_sexe_age_columns_reduced(self, col_source: str, col_output: str):
    self.dataframe_validated = self._process_sexe_age_reduced(self.dataframe_validated, col_source, col_output)
    setattr(self, col_output + "_col", col_output)
    return self.dataframe_validated

  def process_specie_column_reduced(self, col_source: str, col_output: str):
    self.dataframe_validated = self._process_specie_reduced(self.dataframe_validated, col_source, col_output)
    setattr(self, col_output + "_col", col_output)
    return self.dataframe_validated

  def select_columns(self,):
    self.dataframe_validated = self.dataframe_validated[[self.date_declaration_col, self.saison_col, self.heure_col, self.mois_col, self.annee_col,
                                                         self.espece_col, self.federation_col, self.sexe_col, self.age_col, self.photo_dessus_col, self.photo_dessous_col]]
    return self.dataframe_validated

  def select_season(self, season: str):
    self.season_selected = season
    self.season_selected_ = self.season_selected.replace("-", "_")
    self.dataframe_validated = self.dataframe_validated[self.dataframe_validated[self.saison_col] == self.season_selected]
    return self.dataframe_validated

  def create_new_columns(self, list_new_name: list[str]=["espece_sexe", "espece_sexe_reduced", "sexe_age", "sexe_age_reduced","espece_age_reduced",
                    "espece_age", "espece_sexe_age", "espece_sexe_age_reduced"]):
    self.new_columns = list_new_name
    for i in self.new_columns:
        self.dataframe_validated[i] = np.nan
        setattr(self, i + "_col", i)
    return self.dataframe_validated

  @staticmethod
  def _slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")

  @staticmethod
  def _process_sexe_age(dataframe: pd.DataFrame, col_source: str, col_output: str) -> pd.DataFrame:
    serie = dataframe[col_source].copy().fillna("").astype(str).str.strip()
    cond_indet = serie.str.contains(r"\?", na=False) | serie.eq("") | serie.eq("VOL")
    if col_source == "age":
        conds = [
            cond_indet,
            serie.isin(["A", "+1A", "+2A", "Adulte"]),
            serie.isin(["J", "1A", "2A", "Juvénile"]),
        ]
        choices = ["Indéterminé", "Adulte", "Juvénile"]
    elif col_source == "sexe":
        conds = [
            cond_indet,
            serie.isin(["M", "Mâle"]),
            serie.isin(["F", "Femelle"]),
        ]
        choices = ["Indéterminé", "Mâle", "Femelle"]
    else:
        raise ValueError("`kind` doit être 'age' ou 'sexe'.")
    dataframe[col_output] = np.select(conds, choices, default="Indéterminé")
    return dataframe

  @staticmethod
  def _process_sexe_age_reduced(dataframe: pd.DataFrame, col_source: str, col_output: str) -> pd.DataFrame:
    mappings = {
        "age": {
            "Adulte": "A",
            "Juvénile": "J",
            "Indéterminé": "Ind",
            np.nan: "Ind",
        },
        "sexe": {
            "Mâle": "M",
            "Femelle": "F",
            "Indéterminé": "Ind",
            np.nan: "Ind",
        },
    }
    if col_source not in mappings:
        raise ValueError("`kind` doit être 'age' ou 'sexe'.")
    dataframe[col_output] = dataframe[col_source].map(mappings[col_source]).fillna("Ind")
    return dataframe

  @staticmethod
  def _process_specie_reduced(dataframe: pd.DataFrame, col_source: str, col_output: str) -> pd.DataFrame:
    mapping = {
        "Sarcelle d'hiver"    : "SAH",
        "Sarcelle d'été"      : "SAE",
        "Souchet"             : "SOU",
        "Siffleur"            : "SIF",
        "Pilet"               : "PIL",
        "Milouin"             : "MIL",
        "Milouinan"           : "MOU",
        "Morillon"            : "MOR",
        "Chipeau"             : "CHI",
        "Nette rousse"        : "NET",
        "Tourterelle des bois": "TBO",
        "Bécasses des bois"   : "BEC",
        "Caille des blés"     : "CAI",
        "Grive draine"        : "GDR",
        "Grive litorne"       : "GLI",
        "Grive mauvis"        : "GMA",
        "Grive musicienne"    : "GMU"

    }

    dataframe[col_output] = dataframe[col_source].map(mapping)
    return dataframe

  def create_multiclass_columns(self):
    self.dataframe_validated[self.sexe_age_col] = self.dataframe_validated[self.sexe_col] + " / " + self.dataframe_validated[self.age_col]
    self.dataframe_validated[self.espece_sexe_age_col] = self.dataframe_validated[self.espece_col] + " / " + self.dataframe_validated[self.sexe_col] + " / " + self.dataframe_validated[self.age_col]
    self.dataframe_validated[self.espece_sexe_col] = self.dataframe_validated[self.espece_col] + " / " + self.dataframe_validated[self.sexe_col]
    self.dataframe_validated[self.espece_age_col] = self.dataframe_validated[self.espece_col] + " / " + self.dataframe_validated[self.age_col]

    self.dataframe_validated[self.sexe_age_reduced_col] = self.dataframe_validated[self.sexe_reduced_col] + " / " + self.dataframe_validated[self.age_reduced_col]
    self.dataframe_validated[self.espece_sexe_age_reduced_col] = self.dataframe_validated[self.espece_reduced_col] + " / " + self.dataframe_validated[self.sexe_reduced_col] + " / " + self.dataframe_validated[self.age_reduced_col]
    self.dataframe_validated[self.espece_sexe_reduced_col] = self.dataframe_validated[self.espece_reduced_col] + " / " + self.dataframe_validated[self.sexe_reduced_col]
    self.dataframe_validated[self.espece_age_reduced_col] = self.dataframe_validated[self.espece_reduced_col] + " / " + self.dataframe_validated[self.age_reduced_col]
    return self.dataframe_validated

  def _create_final_dataframe(self):
    self.dataframe_final = self.dataframe_validated[[self.date_declaration_col,
                                                     self.saison_col,
                                                     self.federation_col,
                                                     self.photo_dessus_col,
                                                     self.photo_dessous_col,
                                                     self.espece_col,
                                                     self.espece_reduced_col,
                                                     self.sexe_col,
                                                     self.sexe_reduced_col,
                                                     self.age_col,
                                                     self.age_reduced_col,
                                                     self.espece_sexe_col,
                                                     self.espece_sexe_reduced_col,
                                                     self.sexe_age_col,
                                                     self.sexe_age_reduced_col,
                                                     self.espece_age_col,
                                                     self.espece_age_reduced_col,
                                                     self.espece_sexe_age_col,
                                                     self.espece_sexe_age_reduced_col
                                                     ]]
    return self.dataframe_final


  def _create_output_path(self, data_processed_dir: str, specie:str, data_source: str="ISIGEO_ChassAdapt"):
    self.data_processed_dir = data_processed_dir
    self.data_source = data_source.lower()
    self.specie = self.dataframe_validated[self.espece_col].unique()[0]
    self.specie_slug = self._slugify(self.specie)
    self.specie_slug_ = self.specie_slug.replace(" ", "_")
    self.today = datetime.datetime.now().strftime("%Y%m%d")#.strftime("%Y%m%d-%H%M%S")

    self.data_output_name = f"{self.specie_slug_}_{self.today}.csv"
    self.data_ouptput_path = os.path.join(self.data_processed_dir, self.data_output_name)
    print("Save path:", self.data_ouptput_path)
    return self.data_ouptput_path

  def create_and_save_dataframe(self, data_processed_dir: str, specie: str, data_source: str="ISIGEO_ChassAdapt"):
    self.dataframe_final = self._create_final_dataframe()
    self.data_ouptput_path = self._create_output_path(data_processed_dir, specie, data_source)
    self.dataframe_final.to_csv(self.data_ouptput_path, index=False)
    print("Le fichier a été sauvegardé.")
