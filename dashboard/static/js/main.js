// Interactive Prediction AJAX Handler
document.addEventListener("DOMContentLoaded", () => {
    const form = document.getElementById("predictionForm");
    const predictBtn = document.getElementById("predictBtn");
    const resultBox = document.getElementById("predictionResult");
    const resYield = document.getElementById("resYield");
    const resProd = document.getElementById("resProd");
    const resModel = document.getElementById("resModel");

    if (form) {
        form.addEventListener("submit", async (e) => {
            e.preventDefault();

            // Disable button and show spinner
            predictBtn.disabled = true;
            predictBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Running Spark Inference...';

            const payload = {
                state: document.getElementById("state").value,
                season: document.getElementById("season").value,
                year: parseInt(document.getElementById("year").value, 10),
                area: parseFloat(document.getElementById("area").value),
                rainfall: parseFloat(document.getElementById("rainfall").value),
                fertilizer: parseFloat(document.getElementById("fertilizer").value),
                pesticide: parseFloat(document.getElementById("pesticide").value)
            };

            try {
                const response = await fetch("/api/predict", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload)
                });

                const data = await response.json();

                if (response.ok && data.status === "success") {
                    resYield.textContent = data.predicted_yield_tonnes_per_ha.toFixed(4);
                    resProd.textContent = Number(data.estimated_total_production_tonnes).toLocaleString();
                    resModel.textContent = `Engine: ${data.model_used} | State: ${data.state} (${data.season})`;
                    resultBox.style.display = "block";
                    resultBox.scrollIntoView({ behavior: "smooth", block: "nearest" });
                } else {
                    alert("Prediction Error: " + (data.error || "Failed to generate prediction."));
                }
            } catch (err) {
                console.error("Inference Error:", err);
                alert("Failed to connect to the prediction API.");
            } finally {
                predictBtn.disabled = false;
                predictBtn.innerHTML = '<i class="fa-solid fa-wand-magic-sparkles"></i> Predict Cotton Yield';
            }
        });
    }
});
