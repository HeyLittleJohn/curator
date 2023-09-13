import asyncio
import typer
from torch import save
from rl_agent.train import train_agent
from option_bot.utils import timestamp_now

app = typer.Typer()


@app.command()
def train(ticker: str, start_date: str, num_positions: int):
    typer.echo("\nTraining the model... \n ... \n ...")
    agent = asyncio.run(train_agent(ticker, start_date, num_positions))
    typer.echo("Training complete!")
    typer.echo("Saving model...")
    save(agent.state_dict(), f".models/ticker_{ticker}_{start_date}_{num_positions}_{timestamp_now()}.pth")


@app.command()
def test(num: int):
    typer.echo(f"Testing {num}...")


if __name__ == "__main__":
    app()
