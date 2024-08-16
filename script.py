import chess.pgn

with open('2024-08-12.pgn') as pgn:
    first_game = chess.pgn.read_game(pgn)

print(first_game.headers['White'])
