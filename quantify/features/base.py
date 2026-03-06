from abc import ABC, abstractmethod
import pandas as pd

class BaseFeature(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass
    @abstractmethod
    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

class FeatureSet:
    def __init__(self, features: list[BaseFeature]):
        self.features = features

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        for feature in self.features:
            result = pd.concat([result, feature.compute(df)], axis=1)
        return result