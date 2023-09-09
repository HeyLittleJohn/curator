import asyncio
import typer
from torch import save
from rl_agent.train import train_agent

app = typer.Typer()


@app.command()
def train(ticker: str, start_date: str, num_positions: int):
    typer.echo("\nTraining the model... \n ... \n ...")
    agent = asyncio.run(train_agent(ticker, start_date, num_positions))
    typer.echo("Training complete!")
    typer.echo("Saving model...")
    save(agent.state_dict(), f"ticker_{ticker}_{start_date}_{num_positions}.pth")


@app.command()
def test(num: int):
    typer.echo(f"Testing {num}...")


if __name__ == "__main__":
    app()
