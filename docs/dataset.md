# Dataset Profile & Feature Engineering Specifications

## Source Information

- **Dataset Identifier**: `dhyann2815/india-crop-yield-prediction` (Hugging Face Hub)
- **Temporal Coverage**: 2000 to 2026 (27 consecutive agricultural years)
- **Geographic Coverage**: 31 Indian States and Union Territories
- **Crop Types**: 62 distinct agricultural crops
- **Total Records**: 21,750 rows
- **Cotton Records**: 582 rows (`Crop == 'Cotton(lint)'`)

## Dataset Schema

| Column Name | Data Type | Physical Meaning | Summary Range |
| :--- | :--- | :--- | :--- |
| `Year` | Integer | Harvest calendar year | 2000 – 2026 |
| `State` | String | Indian State or Union Territory | 31 distinct entities |
| `Crop` | String | Agricultural crop name | 62 crop varieties |
| `Season` | String | Cropping period | Kharif (550), Whole Year (22), Summer (9), Rabi (1) |
| `Area` | Double | Cultivated land area (hectares) | 1.0 – 4,491,331 ha (Mean: 583,830 ha) |
| `Production` | Double | Harvest output | Excluded from modeling to avoid target leakage |
| `Annual_Rainfall` | Double | Annual cumulative rainfall (mm) | 301.2 – 6,258.8 mm (Mean: 1,378.4 mm) |
| `Fertilizer` | Double | Gross fertilizer consumption (kg) | 35.8 – 771,431,000 kg (Mean: 65,550,840 kg) |
| `Pesticide` | Double | Gross pesticide application (kg) | 0.25 – 1,661,792 kg (Mean: 126,789 kg) |
| `Yield` | Double | **Target variable**: Cotton yield per unit area | 0.0 – 5.295 tonnes/ha (Mean: 1.545, Std: 1.166) |

---

## Data Leakage Elimination Strategy

> [!CAUTION]
> In raw agricultural survey datasets, total `Production` is recorded **after harvest** as an aggregate output. Mathematically, `Yield = Production / Area`. Including `Production` as a predictive feature for `Yield` creates direct target leakage, invalidating the model for pre-harvest agricultural forecasting.

### Explicit Model Feature Allocation

1. **Target Variable**:
   - `Yield` (Continuous numerical target)
2. **Predictive Features Used**:
   - **Categorical**: `State`, `Season` (encoded via `StringIndexer` and `OneHotEncoder`)
   - **Numerical**: `Year`, `Area`, `Annual_Rainfall`, `Fertilizer`, `Pesticide`
   - **Engineered Intensities**:
     * $\text{Fertilizer\_per\_Area} = \frac{\text{Fertilizer}}{\text{Area} + 10^{-5}}$ (kg/ha)
     * $\text{Pesticide\_per\_Area} = \frac{\text{Pesticide}}{\text{Area} + 10^{-5}}$ (kg/ha)
3. **Features Removed**:
   - `Production`: Post-harvest outcome that introduces target leakage.
   - `Crop`: Constant (`Cotton(lint)`) for single-crop cotton specialization.
