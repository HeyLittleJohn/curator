import typer
from torch import save
from rl_agent.train import train_agent

app = typer.Typer()


@app.command()
async def train(ticker: str, start_date: str, num_positions: int):
    typer.echo("Training the model...")
    agent = await train_agent(ticker, start_date, num_positions)
    typer.echo("Training complete!")
    typer.echo("Saving model...")
    save(agent.state_dict(), f"ticker_{ticker}_{start_date}_{num_positions}.pth")


if __name__ == "__main__":
    app()
