import torch
import random
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F

device = "gpu" if torch.cuda.is_available() else "cpu"

print(device)
