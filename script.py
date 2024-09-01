from pprint import pprint
import asyncio

import chess
import chess.pgn
import chess.engine
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
        move_san = board.san(move)
        board.push(move)
        board_fen = board.fen()

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
            game_dict[board_fen]['initial_fen'] = prev_board_fen

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



async def engine_evaluation(prepared_dict: dict) -> dict:
    transport, engine = await chess.engine.popen_uci('/usr/bin/stockfish')
    print('Configuring engine...')
    await engine.configure({'Hash': 12288, 'Threads': 4})
    start, finish = 0, len(prepared_dict)

    result = {}
    for k, v in prepared_dict.items():
        start += 1
        print(f'Analyzing position {start} of {finish} ({start * 100 / finish:.2f}%)...')
        board = chess.Board(k)
        print(f'\t- Position FEN: {board.fen()}')
        print(f'\t- My move: {v["my_most_popular_move"]}')
        original_info = await engine.analyse(board, chess.engine.Limit(depth=20))
        try:
            top_move = board.san(original_info['pv'][0])
        except KeyError:
            top_move = ''
        turn = original_info['score'].turn
        if top_move != v['my_most_popular_move'] and v['my_most_popular_move'] != '' and top_move != '':
            board.push_san(v['my_most_popular_move'])
            my_info = await engine.analyse(board, chess.engine.Limit(depth=20))
            aux = {
                'engine_top_move': top_move,
                'after_my_move_evalutation_result': my_info["score"].wdl().pov(turn).expectation(),
                'before_my_move_evaluation_result': original_info["score"].wdl().pov(turn).expectation(),
            }
        else:
            aux = {
                'engine_top_move': top_move,
            }
        print(f'\t- Engine top move: {top_move}')
        result[k] = v | aux
        if start % 200 == 0:
            print('Salvando resultado parcial...')
            part = pd.DataFrame.from_dict(result, orient='index')
            part.to_parquet(f'{FILE_NAME}_engine_data_part.parquet')
    return result

game_df = pd.DataFrame.from_dict(game_dict, orient='index')
game_df['position_importance'] = (game_df.win_qtd + 2 * game_df.draw_qtd * 4 * game_df.loss_qtd) / 7
game_df = game_df.sort_values('loss_qtd', ascending=False)
game_df.to_parquet(f'{FILE_NAME}_game_data.parquet')
engine_dict = asyncio.run(engine_evaluation(game_df.to_dict(orient='index')))
engine_df = pd.DataFrame.from_dict(engine_dict, orient='index')
engine_df.to_parquet(f'{FILE_NAME}_engine_data.parquet')
