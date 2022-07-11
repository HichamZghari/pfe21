import os
from loader import *
from flask import Flask

app = Flask(__name__)

@app.route("/", endpoint='hello_world')
def hello_world():
    name = os.environ.get("NAME", "World")
    return "Hello welcome on the loader of the atos market place please change the url to reload some data in snowflake!"


@app.route("/ACCIDENT_ROUTIER_CARACTERISTIQUES", endpoint='accident_ca')
def accident_ca():
    arg = "ACCIDENT_ROUTIER_CARACTERISTIQUES"
    load(arg)
    return "Accidents routiers caractéristiques succesfully loaded ! "


@app.route("/ACCIDENT_ROUTIER_USAGERS", endpoint='accident_us')
def accident_us():
    arg = "ACCIDENT_ROUTIER_USAGERS"
    load(arg)
    return "Accidents routiers usagers succesfully loaded ! "


@app.route("/ACCIDENT_ROUTIER_VEHICULES", endpoint='accident_ve')
def accident_ve():
    arg = "ACCIDENT_ROUTIER_VEHICULES"
    load(arg)
    return "Accidents routiers vehicules succesfully loaded ! "


@app.route("/BAN_ADRESSES", endpoint='ban_adresse')
def ban_adresse():
    name = os.environ.get("NAME", "World")
    arg = "BAN_ADRESSES"
    load(arg)
    return "BAN ADRESSES succesfully loaded ! "


@app.route("/BAN_LIEUX_DITS", endpoint='ban_lieux_dits')
def ban_lieux_dits():
    arg = "BAN_LIEUX_DITS"
    load(arg)
    return "BAN lieux dits succesfully loaded ! "


@app.route("/DPE_AMELIORATION", endpoint='dpe_am')
def dpe_am():
    arg = "DPE_AMELIORATION"
    load(arg)
    return "DPE AMELIORATION succesfully loaded ! "


@app.route("/DPE_BAIE", endpoint='dpe_baie')
def dpe_baie():
    arg = "DPE_BAIE"
    load(arg)
    return "DPE BAIE succesfully loaded ! "


@app.route("/DPE_BATIMENT", endpoint='dpe_bat')
def dpe_bat():
    arg = "DPE_BATIMENT"
    load(arg)
    return "DPE BATIMENT succesfully loaded ! "


@app.route("/DPE_CLEAN_GEOCODEC", endpoint='dpe_clean')
def dpe_clean():
    arg = "DPE_CLEAN_GEOCODEC"
    load(arg)
    return "DPE CLEAN GEOCODEC succesfully loaded ! "

@app.route("/DPE_CONSOMMATION", endpoint='dpe_conso')
def dpe_conso():
    arg = "DPE_CONSOMMATION"
    load(arg)
    return "DPE consommation succesfully loaded ! "

@app.route("/DPE_DESCRIPTIF", endpoint='dpe_desc')
def dpe_desc():
    arg = "DPE_DESCRIPTIF"
    load(arg)
    return "DPE descriptif succesfully loaded ! "

@app.route("/DPE_FACTURE", endpoint='dpe_fact')
def dpe_fact():
    arg = "DPE_FACTURE"
    load(arg)
    return "DPE facture succesfully loaded ! "


@app.route("/DPE_FICHE_TECHNIQUE", endpoint='dpe_fiche')
def dpe_fiche():
    arg = "DPE_FICHE_TECHNIQUE"
    load(arg)
    return "DPE fiche technique succesfully loaded ! "


@app.route("/DPE_GENERATEUR_CHAUFFAGE", endpoint='dpe_gen_ch')
def dpe_gen_ch():
    arg = "DPE_GENERATEUR_CHAUFFAGE"
    load(arg)
    return "DPE generateur chauffage succesfully loaded ! "

@app.route("/DPE_GENERATEUR_ECS", endpoint='dpe_gen_ecs')
def dpe_gen_ecs():
    arg = "DPE_GENERATEUR_ECS"
    load(arg)
    return "DPE generateur ecs succesfully loaded ! "


@app.route("/DPE_INSTALLATION_CHAUFFAGE", endpoint='dpe_inst_ch')
def dpe_inst_ch():
    arg = "DPE_INSTALLATION_CHAUFFAGE"
    load(arg)
    return "DPE installation chauffage succesfully loaded ! "



@app.route("/DPE_INSTALLATION_ECS", endpoint='dpe_inst_ecs')
def dpe_inst_ecs():
    arg = "DPE_INSTALLATION_ECS"
    load(arg)
    return "DPE installation ecs succesfully loaded ! "


@app.route("/DPE_MASQUE_SOLAIRE_LOINTAIN", endpoint='dpe_masq_sol')
def dpe_masq_sol():
    arg = "DPE_MASQUE_SOLAIRE_LOINTAIN"
    load(arg)
    return "DPE masque solaire succesfully loaded ! "


@app.route("/DPE_NEUF", endpoint='dpe_neuf')
def dpe_neuf():
    arg = "DPE_NEUF"
    load(arg)
    return "DPE neuf succesfully loaded ! "

@app.route("/DPE_PAROI_OPAQUE", endpoint='dpe_paroi')
def dpe_paroi():
    arg = "DPE_PAROI_OPAQUE"
    load(arg)
    return "DPE paroi opaque succesfully loaded ! "


@app.route("/DPE_PONT_THERMIQUE", endpoint='dpe_pont')
def dpe_pont():
    arg = "DPE_PONT_THERMIQUE"
    load(arg)
    return "DPE pont thermique succesfully loaded ! "



@app.route("/DPE_PRODUCTION_ENERGIE", endpoint='dpe_prod')
def dpe_prod():
    arg = "DPE_PRODUCTION_ENERGIE"
    load(arg)
    return "DPE production energie succesfully loaded ! "

@app.route("/FINESS", endpoint='finess')
def finess():
    name = os.environ.get("NAME", "World")
    arg = "FINESS"
    load(arg)
    return "FINESS succesfully loaded ! "


@app.route("/OPEN_FOOD", endpoint='food')
def food():
    arg = "OPEN_FOOD"
    load(arg)
    return "OPEN_FOOD succesfully loaded ! "




@app.route("/POSTAL", endpoint='postal')
def postal():
    name = os.environ.get("NAME", "World")
    arg = "POSTAL"
    load(arg)
    return "Postal succesfully loaded !"


@app.route("/SIREN_ETABLISSEMENT", endpoint='siren_e')
def siren_e():
    arg = "SIREN_ETABLISSEMENT"
    load(arg)
    return "Sirene etablissement succesfully loaded !"


@app.route("/SIREN_UNITE_LEGALE", endpoint='siren_u')
def siren_u():
    arg = "SIREN_UNITE_LEGALE"
    load(arg)
    return "Sirene unite legale succesfully loaded !"


@app.route("/LOGEMENT_VALEUR_FONCIERE", endpoint='logement_vf')
def logement_vf():
    arg = "LOGEMENT_VALEUR_FONCIERE"
    load(arg)
    return "LOGEMENT VF succesfully loaded !"



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))