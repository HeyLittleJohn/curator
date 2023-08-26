from pandas import DataFrame
import torch

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


def dataframe_to_dict(df: DataFrame, index_key: str) -> list[dict]:
    """Convert dataframe to list of dicts"""
    return df.set_index(index_key).to_dict("index")


def dataframe_to_tensor(df: DataFrame) -> torch.Tensor:
    """Convert dataframe to tensor"""
    return torch.tensor(df.to_numpy(), device=device, dtype=torch.float32)


def convert_dicts_to_tensors(list_of_dicts: list[dict]) -> list[torch.Tensor]:
    """Convert list of dicts to list of tensors"""
    return [
        torch.tensor(list_of_dicts[i].values(), device=device, dtype=torch.float32) for i in range(len(list_of_dicts))
    ]
