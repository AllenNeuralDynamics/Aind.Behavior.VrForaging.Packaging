from pathlib import Path

import pandas as pd
from aind_behavior_vr_foraging.data_contract import dataset

from aind_behavior_vr_foraging_packaging.processing import (
    TrialTableProcessor,
)

dataset_path = Path(r"C:\Users\bruno.cruz\Desktop\815103_2026-05-08T231548Z")
ds = dataset(dataset_path)
ttp = TrialTableProcessor(ds, raise_on_error=True)
sites = ttp.process_to_sites()
sites_df = pd.DataFrame([s.model_dump() for s in sites])

print(sites_df.head())

rewarded_sites = sites_df[sites_df["site_label"] == "RewardSite"]
for patch_id in rewarded_sites["patch_label"].unique():
    patch_data = rewarded_sites[rewarded_sites["patch_label"] == patch_id]
    p_choice = patch_data["has_choice"].mean()
    p_reward = patch_data["has_reward"].sum() / len(patch_data)
    print(f"Patch {patch_id}: P(choice)={p_choice:.2f}, P(reward|choice)={p_reward:.2f}")
