from pprint import pprint

import chess.pgn
import pandas as pd

GAMER = 'ChaosJournaler'
FILE_NAME = '2024-08-30'

game_dict = {}
oponent_move_dict = {}
my_move_dict = {}
i = 0

pgn = open(f'{FILE_NAME}.pgn')

while True:
    game = chess.pgn.read_game(pgn)

    if game is None:
        break

    i += 1
    print(f'Processando o jogo {i}...')
    playing_white, playing_black = False, False
    if game.headers['White'] == GAMER:
        playing_white = True
    elif game.headers['Black'] == GAMER:
        playing_black = True
    else:
        raise ValueError(f'Gamer {GAMER} not found.')

    white_won, white_loss, tie = False, False, False
    result = game.headers['Result']
    if result == '1-0':
        white_won = True
    elif result == '0-1':
        white_loss = True
    elif result == '1/2-1/2':
        tie = True
    else:
        raise ValueError(f'Invalid Result: {result}')

    if tie:
        key = 'draw_qtd'
    elif playing_white and white_won or playing_black and white_loss:
        key = 'win_qtd'
    elif playing_white and white_loss or playing_black and white_won:
        key = 'loss_qtd'
    else:
        raise ValueError('The result coudn\'t be determined')

    board = game.board()

    prev_board_fen = 'invalid fen'

    for move in game.mainline_moves():
        board_fen = board.fen()
        move_san = board.san(move)
        board.push(move)

        my_turn = (
                (board.turn == chess.WHITE and playing_white) or 
                (board.turn == chess.BLACK and playing_black)
        )
        
        if my_turn:
            try:
                game_dict[board_fen]
            except KeyError:
                game_dict[board_fen] = dict(
                    draw_qtd=0, win_qtd=0, loss_qtd=0
                )
                oponent_move_dict[board_fen] = {}
                my_move_dict[board_fen] = {}
            game_dict[board_fen][key] += 1

            try:
                oponent_move_dict[board_fen][move_san] += 1
            except KeyError:
                oponent_move_dict[board_fen][move_san] = 1
        else:
            if prev_board_fen != 'invalid fen':
                try:
                    my_move_dict[prev_board_fen][move_san] += 1
                except KeyError:
                    my_move_dict[prev_board_fen][move_san] = 1

        prev_board_fen = board_fen

for k, v in oponent_move_dict.items():
    prev_max_key = ''
    prev_max_value = 0
    for key, value in v.items():
        if value > prev_max_value:
            prev_max_key = key
            prev_max_value = value
    game_dict[k]['most_popular_last_move'] = prev_max_key

for k, v in my_move_dict.items():
    prev_max_key = ''
    prev_max_value = 0
    for key, value in v.items():
        if value > prev_max_value:
            prev_max_key = key
            prev_max_value = value
    game_dict[k]['my_most_popular_move'] = prev_max_key

data = pd.DataFrame.from_dict(game_dict, orient='index')
data['position_importance'] = (data.win_qtd + 2 * data.draw_qtd * 4 * data.loss_qtd) / 7
# data = data.sort_values('loss_qtd', ascending=False)
data.to_parquet(f'{FILE_NAME}.parquet')
