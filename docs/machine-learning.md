# Machine Learning & Model Evaluation

## Problem Formulation

Predicting cotton crop yield is formulated as a continuous regression task:
$$y = f(\mathbf{x}) + \epsilon$$
where:
- $y \in \mathbb{R}^+$ represents `Yield` (tonnes or bales per hectare).
- $\mathbf{x} \in \mathbb{R}^D$ represents input agricultural, climate, and geographic features.
- $\epsilon$ is the stochastic residual error.

---

## Machine Learning Models Evaluated

Three distinct algorithms from **Spark MLlib** were trained and evaluated on an 80/20 holdout split ($N_{\text{train}} = 492, N_{\text{test}} = 90$, seed = 42):

1. **Linear Regression with ElasticNet Regularization**:
   - Baseline parametric model.
   - Regularization: `regParam = 0.05`, `elasticNetParam = 0.2` (L1/L2 combination to control collinearity among rainfall and input quantities).
2. **Random Forest Regressor**:
   - Non-parametric ensemble of 60 decision trees (`maxDepth = 8`).
   - Handles multi-modal non-linear relationships between rainfall thresholds, state microclimates, and fertilizer saturation.
3. **Gradient-Boosted Trees (GBT) Regressor**:
   - Sequential boosting ensemble of 50 trees (`maxDepth = 5`).
   - Optimizes squared error loss sequentially.

---

## Empirical Evaluation Results

Evaluated on the exact holdout test set using PySpark `RegressionEvaluator`:

| Model Architecture | RMSE (Root Mean Squared Error) | MAE (Mean Absolute Error) | $R^2$ (Coefficient of Determination) | Selection Outcome |
| :--- | :---: | :---: | :---: | :---: |
| **Random Forest Regressor** | **0.6687** | **0.3823** | **0.6160** | **SELECTED BEST MODEL** |
| **Linear Regression** | 0.7926 | 0.4629 | 0.4605 | Candidate Baseline |
| **Gradient-Boosted Trees** | 0.9066 | 0.4692 | 0.2941 | Candidate |

---

## Model Selection Rationale

1. **Superior Accuracy**: Random Forest achieved the lowest RMSE (0.6687) and lowest MAE (0.3823).
2. **Variance Explanation**: Random Forest explains **61.60%** of the variance in unseen cotton yields without relying on target leakage variables (`Production`).
3. **Generalization Stability**: GBT suffered from slight overfitting on the finite state-level dataset ($R^2 = 0.2941$), whereas Random Forest bagging averaged out tree variance cleanly.
4. **Export Artifact**: The winning `PipelineModel` is stored at `models/best_model/` and exported to the inference engine.
