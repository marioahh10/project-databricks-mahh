# project-databricks-mahh
ETL con Databricks y arquitectura Medallion usando Kaggle Ecommerce + Young's Modulus
Este repositorio contiene el diseño de un pipeline ETL basado en **Databricks** y la arquitectura **Medallion (Bronze, Silver y Gold)**.

Se utilizan **dos datasets de Kaggle**:

1. **Ecommerce Sales Prediction Dataset**  
   Archivo: `Ecommerce_Sales_Prediction_Dataset.csv`  
   Contiene información de ventas de un e-commerce:
   - `Date`
   - `Product_Category`
   - `Price`
   - `Discount`
   - `Customer_Segment`
   - `Marketing_Spend`
   - `Units_Sold`

2. **Young's Modulus Dataset**  
   Archivo: `youngs_modulus.csv`  
   Contiene información de materiales y su módulo de Young.  
   Viene en una única columna con valores separados por `;`:
   - `Metal;Symbol;Young's Modulus (GPa)`

El objetivo es:

- Construir una **tabla de hechos de ventas** a partir del dataset de e-commerce.
- Construir una **dimensión de materiales** a partir del dataset de Young's modulus.
- Generar **tablas de KPIs** para dashboards en la capa Gold.
- Organizar todo el código en GitHub siguiendo buenas prácticas de un Lakehouse con Databricks.

---

## Arquitectura Medallion

```text
Kaggle CSV (landing)
        ↓
RAW  (Data Lake - ADLS)
        ↓
Bronze (tablas Delta crudas)
        ↓
Silver (fact_sales, dim_material)
        ↓
Gold   (tablas de KPIs)
        ↓
Dashboards (Power BI / Databricks SQL)
