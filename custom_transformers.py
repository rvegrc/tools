from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import PowerTransformer

class SafePowerTransformer(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.pt = {}  # Dictionary to store transformers by column name
        self.columns = []  # Store column names for reference

    def fit(self, X, y=None):
        self.pt = {}  # Reset in case fit is called multiple times
        self.columns = X.columns.tolist()  # Store column names
        for col in self.columns:
            pt = PowerTransformer()
            try:
                pt.fit(X[[col]])
                self.pt[col] = pt
            except Exception:
                # Use a placeholder for columns that can't be transformed
                self.pt[col] = "no_transform"
        return self

    def transform(self, X):
        X_transformed = X.copy()  # Avoid modifying the original DataFrame
        for col in self.columns:
            if self.pt[col] != "no_transform":
                X_transformed[col] = self.pt[col].transform(X[[col]])
        return X_transformed