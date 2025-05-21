import random
import logging

# --- Basic Logging for this conceptual script ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# --- Core Parameters (would come from config in the full POC) ---
TARGET_OVERALL_SELECTION_RATE = 0.01  # 1%
ESTIMATED_UNPROTECTED_RATIO = 0.80   # 80% are not protected

# --- Calculate Derived Probability ---
# This is the probability applied to each eligible (unprotected, not previously selected) item.
DERIVED_PROBABILITY_FOR_UNPROTECTED = TARGET_OVERALL_SELECTION_RATE / ESTIMATED_UNPROTECTED_RATIO
if DERIVED_PROBABILITY_FOR_UNPROTECTED > 1.0:
    logging.warning(f"Derived probability ({DERIVED_PROBABILITY_FOR_UNPROTECTED*100:.2f}%) > 100%. Capping at 100%.")
    DERIVED_PROBABILITY_FOR_UNPROTECTED = 1.0
elif DERIVED_PROBABILITY_FOR_UNPROTECTED < 0: # Should not happen with valid inputs
    logging.warning(f"Derived probability ({DERIVED_PROBABILITY_FOR_UNPROTECTED*100:.2f}%) is <= 0. No selections by probability.")
    DERIVED_PROBABILITY_FOR_UNPROTECTED = 0.0

logging.info(f"Conceptual Derived Probability for Unprotected Items: {DERIVED_PROBABILITY_FOR_UNPROTECTED*100:.4f}%")

def should_select_policy(policy_data, previously_selected_ids_global):
    """
    Determines if a single policy should be selected based on the new logic.

    Args:
        policy_data (dict): A dictionary representing a single application.
                            Expected keys: 'id' (str), 'protected_class' (int, 0 or 1).
        previously_selected_ids_global (set): A set of all policy IDs selected in any past run.

    Returns:
        bool: True if the policy is selected for review, False otherwise.
    """
    policy_id = policy_data.get('id')
    is_protected = policy_data.get('protected_class') == 1 # Assuming 1 means protected

    logging.info(f"\nEvaluating Policy ID: {policy_id}")

    # 1. Check Protected Class
    if is_protected:
        logging.info(f"Policy {policy_id}: SKIPPED - Is in protected class.")
        return False

    # 2. Check Non-Reselection (Historical Data Check)
    if policy_id in previously_selected_ids_global:
        logging.info(f"Policy {policy_id}: SKIPPED - Already selected in a previous run.")
        return False

    # 3. Apply Derived Probability (to non-protected, non-historical items)
    if DERIVED_PROBABILITY_FOR_UNPROTECTED > 0 and random.random() < DERIVED_PROBABILITY_FOR_UNPROTECTED:
        logging.info(f"Policy {policy_id}: SELECTED - Passed derived probability ({DERIVED_PROBABILITY_FOR_UNPROTECTED*100:.4f}%).")
        return True
    else:
        logging.info(f"Policy {policy_id}: NOT SELECTED - Did not pass derived probability.")
        return False
