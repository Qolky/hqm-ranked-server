use tracing::info;
use deadpool_postgres::Pool;

use migo_hqm_server::hqm_game::{HQMGame, HQMTeam};
use migo_hqm_server::hqm_ranked_util::{HQMRanked, HQMRankedConfiguration, HQMPuckTouch};
use migo_hqm_server::hqm_server::{
    HQMServer, HQMServerBehaviour, HQMServerPlayerIndex, HQMSpawnPoint,
};
use migo_hqm_server::hqm_simulate::HQMSimulationEvent;
use std::collections::HashMap;
use std::rc::Rc;
use migo_hqm_server::hqm_ranked_util::State;
use std::collections::hash_map::Entry;
use migo_hqm_server::hqm_game::HQMObjectIndex;

use migo_hqm_server::hqm_game::HQMPuck;

use std::collections::VecDeque;

pub struct HQMRankedBehaviour {
    pub m: HQMRanked,
    pub spawn_point: HQMSpawnPoint,
    pub(crate) team_switch_timer: HashMap<HQMServerPlayerIndex, u32>,
    pub team_max: usize,
}

impl HQMRankedBehaviour {
    pub fn new(
        config: HQMRankedConfiguration,
        team_max: usize,
        spawn_point: HQMSpawnPoint,
        pool: Pool,
    ) -> Self {    
        HQMRankedBehaviour {
            m: HQMRanked::new(config, pool),
            spawn_point,
            team_switch_timer: Default::default(),
            team_max,
        }
    }

    fn update_players(&mut self, server: &mut HQMServer) {
        self.m.update_players(server);
    }

    pub(crate) fn force_player_off_ice(
        &mut self,
        server: &mut HQMServer,
        admin_player_index: HQMServerPlayerIndex,
        force_player_index: HQMServerPlayerIndex,
    ) {
        if let Some(player) = server.players.get(admin_player_index) {
            if player.is_admin {
                let admin_player_name = player.player_name.clone();

                if let Some(force_player) = server.players.get(force_player_index) {
                    let force_player_name = force_player.player_name.clone();
                    if server.move_to_spectator(force_player_index) {
                        let msg = format!(
                            "{} forced off ice by {}",
                            force_player_name, admin_player_name
                        );
                        info!(
                            "{} ({}) forced {} ({}) off ice",
                            admin_player_name,
                            admin_player_index,
                            force_player_name,
                            force_player_index
                        );
                        server.messages.add_server_chat_message(msg);
                        self.team_switch_timer.insert(force_player_index, 500);
                    }
                }
            } else {
                server.admin_deny_message(admin_player_index);
                return;
            }
        }
    }

    pub(crate) fn set_team_size(
        &mut self,
        server: &mut HQMServer,
        player_index: HQMServerPlayerIndex,
        size: &str,
    ) {
        if let Some(player) = server.players.get(player_index) {
            if player.is_admin {
                if let Ok(new_num) = size.parse::<usize>() {
                    if new_num > 0 && new_num <= 15 {
                        self.team_max = new_num;

                        info!(
                            "{} ({}) set team size to {}",
                            player.player_name, player_index, new_num
                        );
                        let msg = format!("Team size set to {} by {}", new_num, player.player_name);

                        server.messages.add_server_chat_message(msg);
                    }
                }
            } else {
                server.admin_deny_message(player_index);
            }
        }
    }
}

impl HQMServerBehaviour for HQMRankedBehaviour {
    fn init(&mut self, server: &mut HQMServer) {
        server.history_length = 1000;
    }

    fn before_tick(&mut self, server: &mut HQMServer) {
        self.update_players(server);
    }

    fn after_tick(&mut self, server: &mut HQMServer, events: &[HQMSimulationEvent]) {
        self.m.after_tick(server, events);
    }

    fn handle_command(
        &mut self,
        server: &mut HQMServer,
        command: &str,
        arg: &str,
        player_index: HQMServerPlayerIndex,
    ) {
        match command {
            "set" => {
                let args = arg.split(" ").collect::<Vec<&str>>();
                if args.len() > 1 {
                    match args[0] {
                        "redscore" => {
                            if let Ok(input_score) = args[1].parse::<u32>() {
                                self.m
                                    .set_score(server, HQMTeam::Red, input_score, player_index);
                            }
                        }
                        "bluescore" => {
                            if let Ok(input_score) = args[1].parse::<u32>() {
                                self.m
                                    .set_score(server, HQMTeam::Blue, input_score, player_index);
                            }
                        }
                        "period" => {
                            if let Ok(input_period) = args[1].parse::<u32>() {
                                self.m.set_period(server, input_period, player_index);
                            }
                        }
                        "periodnum" => {
                            if let Ok(input_period) = args[1].parse::<u32>() {
                                self.m.set_period_num(server, input_period, player_index);
                            }
                        }
                        "clock" => {
                            let time_part_string = match args[1].parse::<String>() {
                                Ok(time_part_string) => time_part_string,
                                Err(_) => {
                                    return;
                                }
                            };

                            fn parse_t(
                                s: &str,
                            ) -> Result<(u32, u32, u32), std::num::ParseIntError>
                            {
                                let (time_minutes, rest) =
                                    if let Some((time_minutes, rest)) = s.split_once(':') {
                                        (time_minutes.parse::<u32>()?, rest)
                                    } else {
                                        (0, s)
                                    };
                                let (time_seconds, time_centis) =
                                    if let Some((time_seconds, time_centis)) = rest.split_once(".")
                                    {
                                        let mut centis = time_centis.parse::<u32>()?;
                                        if time_centis.len() == 1 {
                                            centis *= 10;
                                        }
                                        (time_seconds.parse::<u32>()?, centis)
                                    } else {
                                        (rest.parse::<u32>()?, 0)
                                    };
                                Ok((time_minutes, time_seconds, time_centis))
                            }

                            if let Ok((time_minutes, time_seconds, time_centis)) =
                                parse_t(&time_part_string)
                            {
                                self.m.set_clock(
                                    server,
                                    (time_minutes * 100 * 60) + (time_seconds * 100) + time_centis,
                                    player_index,
                                );
                            }
                        }
                        "icing" => {
                            if let Some(arg) = args.get(1) {
                                self.m.set_icing_rule(server, player_index, arg);
                            }
                        }
                        "offside" => {
                            if let Some(arg) = args.get(1) {
                                self.m.set_offside_rule(server, player_index, arg);
                            }
                        }
                        "twolinepass" => {
                            if let Some(arg) = args.get(1) {
                                self.m.set_twoline_pass(server, player_index, arg);
                            }
                        }
                        "offsideline" => {
                            if let Some(arg) = args.get(1) {
                                self.m.set_offside_line(server, player_index, arg);
                            }
                        }
                        "mercy" => {
                            if let Some(arg) = args.get(1) {
                                self.m.set_mercy_rule(server, player_index, arg);
                            }
                        }
                        "first" => {
                            if let Some(arg) = args.get(1) {
                                self.m.set_first_to_rule(server, player_index, arg);
                            }
                        }
                        "teamsize" => {
                            if let Some(arg) = args.get(1) {
                                self.set_team_size(server, player_index, arg);
                            }
                        }
                        "replay" => {
                            if let Some(arg) = args.get(1) {
                                server.set_replay(player_index, arg);
                            }
                        }
                        "goalreplay" => {
                            if let Some(arg) = args.get(1) {
                                self.m.set_goal_replay(server, player_index, arg);
                            }
                        }
                        _ => {}
                    }
                }
            }
            "faceoff" => {
                self.m.faceoff(server, player_index);
            }
            "start" | "startgame" => {
                self.m.start_game(server, player_index);
            }
            "reset" | "resetgame" => {
                self.m.reset_game(server, player_index);
            }
            "pause" | "pausegame" => {
                self.m.pause(server, player_index);
            }
            "unpause" | "unpausegame" => {
                self.m.unpause(server, player_index);
            }
            "sp" | "setposition" => {
                self.m
                    .set_preferred_faceoff_position(server, player_index, arg);
            }
            "fs" => {
                if let Ok(force_player_index) = arg.parse::<HQMServerPlayerIndex>() {
                    self.force_player_off_ice(server, player_index, force_player_index);
                }
            }
            "icing" => {
                self.m.set_icing_rule(server, player_index, arg);
            }
            "offside" => {
                self.m.set_offside_rule(server, player_index, arg);
            }
            "rules" => {
                self.m.msg_rules(server, player_index);
            }
            "1/3" | "g" => {
                self.m.onethree(server, player_index);
            }
            "iamg" => {
                self.m.iamg(server, player_index);
            }
            "login" | "l" => {
                self.m.login(server, player_index, arg);
            }
            "report" => {
                if let Ok(report_player_index) = arg.parse::<HQMServerPlayerIndex>() {
                    self.m.send_report(server, player_index, report_player_index);
                }
            }
            "pick" | "p" | "v" => {
                self.m.pick(server, player_index, arg);
            }
            "lp" | "listpicks" => {
                self.m.send_available_picks_command(server, player_index);
            }
            _ => {}
        };
    }

    fn create_game(&mut self) -> HQMGame {
        self.m.create_game()
    }

    fn before_player_exit(&mut self, _server: &mut HQMServer, player_index: HQMServerPlayerIndex) {
        self.m.cleanup_player(player_index);
        self.team_switch_timer.remove(&player_index);
        self.m.rhqm_game.rejoin_timer.remove(&player_index);
        self.m.verified_players.remove(&player_index);

        if let Some(player) = _server.players.get(player_index) {
            self.m.queued_players
                .retain(|x| x.player_index != player_index);

            if matches!(self.m.status, State::Game { .. })
                || matches!(self.m.status, State::CaptainsPicking { .. })
            {
                if let Some(rhqm_player) = self.m.rhqm_game.get_player_by_index_mut(player_index) {
                    rhqm_player.player_index = None;
                    let left_seconds = rhqm_player.left_seconds;
                    let secs = left_seconds % 60;
                    let minutes = (left_seconds - secs) / 60;
                    let msg = format!(
                        "[Server] {} have {}m {}s to rejoin",
                        player.player_name, minutes, secs
                    );
                    if !_server.game.game_over {
                        _server.messages.add_server_chat_message(msg);
                    }
                }
            }
        }
    }

    fn get_number_of_players(&self) -> u32 {
        self.team_max as u32
    }

    fn save_replay_data(&self, server: &HQMServer) -> bool {
        server.game.period > 0
    }
}

fn add_player(
    m: &mut HQMRanked,
    player_index: HQMServerPlayerIndex,
    player_name: Rc<String>,
    server: &mut HQMServer,
    team: HQMTeam,
    spawn_point: HQMSpawnPoint,
    player_count: &mut usize,
    team_max: usize,
) {
    if *player_count >= team_max {
        return;
    }

    if server
        .spawn_skater_at_spawnpoint(player_index, team, spawn_point)
        .is_some()
    {
        info!(
            "{} ({}) has joined team {:?}",
            player_name, player_index, team
        );
        *player_count += 1;

        m.clear_started_goalie(player_index);
    }
}

pub fn add_touch(
    puck: &HQMPuck,
    entry: Entry<HQMObjectIndex, VecDeque<HQMPuckTouch>>,
    player_index: HQMServerPlayerIndex,
    skater_index: HQMObjectIndex,
    team: HQMTeam,
    time: u32,
) {
    let puck_pos = puck.body.pos.clone();
    let puck_speed = puck.body.linear_velocity.norm();

    let touches = entry.or_insert_with(|| VecDeque::new());
    let most_recent_touch = touches.front_mut();

    match most_recent_touch {
        Some(most_recent_touch)
            if most_recent_touch.player_index == player_index && most_recent_touch.team == team =>
        {
            most_recent_touch.puck_pos = puck_pos;
            most_recent_touch.last_time = time;
            most_recent_touch.puck_speed = puck_speed;
        }
        _ => {
            touches.truncate(15);
            touches.push_front(HQMPuckTouch {
                player_index,
                skater_index,
                team,
                puck_pos,
                puck_speed,
                first_time: time,
                last_time: time,
            });
        }
    }
}
