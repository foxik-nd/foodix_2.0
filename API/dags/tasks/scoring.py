def prepare_training_data(**context):
    """
    WIP – on se contente de logger.
    Place-holder pour la future extraction de features Spark → Parquet.
    """
    log.info("[WIP] prepare_training_data appelé – aucune action effectuée.")


def train_nova_model(**context):
    """
    WIP – aucun entraînement réel.
    À terme : charger le Parquet /tmp/foodix_train, entraîner XGBoost
    et sauver /tmp/nova_model.json
    """
    log.info("[WIP] train_nova_model appelé – aucun modèle entraîné.")


def evaluate_and_register(dry_run: bool = False, **context):
    """
    WIP – retourne toujours 0.0 afin que le BranchPythonOperator
    choisisse la branche « skip_retrain ».
    Le jour où le code sera complet :
      • charger /tmp/nova_model.json
      • évaluer F1
      • (si dry_run) retourner improvement
      • sinon log + MLflow register
    """
    log.info("[WIP] evaluate_and_register appelé – dry_run=%s", dry_run)
    return 0.0
