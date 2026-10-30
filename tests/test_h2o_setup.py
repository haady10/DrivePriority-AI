
import numpy as np
import pandas as pd
import h2o
from h2o.estimators import H2OGradientBoostingEstimator

# Generate artificial data for testing
rng = np.random.default_rng(42)
n = 200

data = pd.DataFrame({
    "smart_5": rng.integers(0, 100, n),
    "smart_187": rng.integers(0, 50, n),
})

data["failure"] = (
    data["smart_5"] + 2 * data["smart_187"] > 90
).astype(int)

# Start H2O
h2o.init(max_mem_size="2G")

# Convert data to H2O format
frame = h2o.H2OFrame(data)
frame["failure"] = frame["failure"].asfactor()

# Train a small GBM model
model = H2OGradientBoostingEstimator(
    ntrees=5,
    max_depth=3,
    seed=42
)

model.train(
    x=["smart_5", "smart_187"],
    y="failure",
    training_frame=frame
)

print("\nSUCCESS: H2O GBM model trained!")
print("Model ID:", model.model_id)
print("Training AUC:", model.auc(train=True))

h2o.cluster().shutdown(prompt=False)
