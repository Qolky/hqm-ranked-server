use crate::hqm_game::{
    HQMGame, HQMGameWorld, HQMObjectIndex, HQMPhysicsConfiguration, HQMPuck, HQMRinkFaceoffSpot,
    HQMRinkLine, HQMRinkSide, HQMRulesState, HQMTeam,
};
use crate::hqm_server::HQMSpawnPoint;
use crate::hqm_server::{HQMServer, HQMServerPlayer, HQMServerPlayerIndex, HQMServerPlayerList};
use crate::hqm_simulate::HQMSimulationEvent;
use bbt::{Rater, Rating};
use deadpool_postgres::Pool;
use itertools::Itertools;
use nalgebra::{Point3, Rotation3, Vector3};
use rand::prelude::SliceRandom;
use reqwest;
use smallvec::SmallVec;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::f32::consts::FRAC_PI_2;
use tracing::info;

#[derive(Clone)]
pub struct RHQMQueuePlayer {
    pub player_id: i32,
    pub player_name: String,
    pub player_index: HQMServerPlayerIndex,
    pub afk: bool,
}

#[derive(Eq, PartialEq, Debug)]
pub enum State {
    Waiting {
        waiting_for_response: bool,
    },
    CaptainsPicking {
        time_left: u32,
        options: Vec<(String, String, i32, i32)>,
        current_team: HQMTeam,
    },
    Game {
        paused: bool,
    },
}

pub enum RHQMGameGoalie {
    TakeTurns {
        first: String,
        second: String,
        third: String,
        overtime: String,
    },
    Fixed {
        goalie: String,
    },
    Undefined,
}

#[derive(Clone)]
pub struct GameEvent {
    pub player_id: i32,
    pub type_id: u32,
    pub speed: f32,
    pub time: u32,
    pub period: u32,
}

#[derive(Clone)]
pub struct RHQMGamePlayer {
    pub player_id: i32,
    pub player_name: String,
    pub player_index: Option<HQMServerPlayerIndex>,
    pub player_points: i64,
    pub player_team: Option<HQMTeam>,
    pub goals: u32,
    pub assists: u32,
    pub left_seconds: u32,
    pub koef: isize,
    pub touches: u32,
    pub winrate: f64,
    pub rating: i32,
}

pub enum PostgresResponse {
    LoginFailed {
        player_index: HQMServerPlayerIndex,
    },
    LoginSuccessful {
        player_id: i32,
        player_index: HQMServerPlayerIndex,
        old_nickname: String,
    },
    LoginBanned {
        player_index: HQMServerPlayerIndex,
    },
    TooManyReports {
        player_index: HQMServerPlayerIndex,
    },
    PlayerReported {
        to_name: String,
        from_name: String,
    },
    PlayerData {
        data: HashMap<i32, (i64, f64, i32)>,
    },
    DailyEvent {
        player_index: HQMServerPlayerIndex,
        text: String,
    },
}

pub struct RHQMGame {
    pub(crate) notify_timer: usize,

    pub(crate) need_to_send: bool,

    pub(crate) game_players: Vec<RHQMGamePlayer>,
    pub events: Vec<GameEvent>,

    pub(crate) xpoints: Vec<f32>,
    pub(crate) zpoints: Vec<f32>,
    pub(crate) data_saved: bool,

    pub rejoin_timer: HashMap<HQMServerPlayerIndex, u32>,

    pub(crate) red_captain: Option<i32>,
    pub(crate) blue_captain: Option<i32>,

    pub(crate) red_timout: usize,
    pub(crate) blue_timout: usize,
    pub(crate) red_resign_votes: Vec<String>,
    pub(crate) blue_resign_votes: Vec<String>,
    pub(crate) red_goalie: RHQMGameGoalie,
    pub(crate) blue_goalie: RHQMGameGoalie,

    pub(crate) pick_number: usize,
}

fn limit(s: &str) -> String {
    if s.len() <= 15 {
        s.to_string()
    } else {
        let s = s[0..13].trim();
        format!("{}..", s)
    }
}

impl RHQMGame {
    pub(crate) fn get_player_by_index(
        &self,
        player_index: HQMServerPlayerIndex,
    ) -> Option<&RHQMGamePlayer> {
        self.game_players
            .iter()
            .find(|x| x.player_index == Some(player_index))
    }

    pub fn get_player_by_index_mut(
        &mut self,
        player_index: HQMServerPlayerIndex,
    ) -> Option<&mut RHQMGamePlayer> {
        self.game_players
            .iter_mut()
            .find(|x| x.player_index == Some(player_index))
    }

    pub(crate) fn get_player_by_id(&self, player_id: i32) -> Option<&RHQMGamePlayer> {
        self.game_players.iter().find(|x| x.player_id == player_id)
    }

    pub(crate) fn get_player_by_id_mut(&mut self, player_id: i32) -> Option<&mut RHQMGamePlayer> {
        self.game_players
            .iter_mut()
            .find(|x| x.player_id == player_id)
    }

    pub(crate) fn new() -> Self {
        Self {
            notify_timer: 0,

            need_to_send: false,
            game_players: Vec::new(),

            xpoints: Vec::new(),
            zpoints: Vec::new(),
            data_saved: false,

            rejoin_timer: Default::default(),
            red_captain: None,
            blue_captain: None,
            red_timout: 6000,
            blue_timout: 6000,
            red_resign_votes: Vec::new(),
            blue_resign_votes: Vec::new(),

            red_goalie: RHQMGameGoalie::Undefined,
            blue_goalie: RHQMGameGoalie::Undefined,
            events: Vec::new(),

            pick_number: 0,
        }
    }
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum RankedPickingMode {
    ServerPick,
    CaptainsPick,
}

pub struct HQMRankedConfiguration {
    pub time_period: u32,
    pub time_warmup: u32,
    pub time_break: u32,
    pub time_intermission: u32,
    pub mercy: u32,
    pub first_to: u32,
    pub periods: u32,
    pub offside: HQMOffsideConfiguration,
    pub icing: HQMIcingConfiguration,
    pub offside_line: HQMOffsideLineConfiguration,
    pub twoline_pass: HQMTwoLinePassConfiguration,
    pub warmup_pucks: usize,
    pub physics_config: HQMPhysicsConfiguration,
    pub blue_line_location: f32,
    pub use_mph: bool,
    pub goal_replay: bool,

    pub picking_mode: RankedPickingMode,
    pub notification: bool,
    pub team_max: usize,
}

pub enum HQMRankedEvent {
    Goal {
        team: HQMTeam,
        goal: Option<HQMServerPlayerIndex>,
        assist: Option<HQMServerPlayerIndex>,
        speed: Option<f32>, // Raw meter/game tick (so meter per 1/100 of a second)
        speed_across_line: f32,
        time: u32,
        period: u32,
    },
}

pub struct HQMRanked {
    pub config: HQMRankedConfiguration,
    pub paused: bool,
    pub(crate) pause_timer: u32,
    is_pause_goal: bool,
    next_faceoff_spot: HQMRinkFaceoffSpot,
    icing_status: HQMIcingStatus,
    offside_status: HQMOffsideStatus,
    twoline_pass_status: HQMTwoLinePassStatus,
    pass: Option<HQMPass>,
    pub(crate) preferred_positions: HashMap<HQMServerPlayerIndex, String>,

    pub started_as_goalie: Vec<HQMServerPlayerIndex>,
    faceoff_game_step: u32,
    step_where_period_ended: u32,
    too_late_printed_this_period: bool,
    start_next_replay: Option<(u32, u32, Option<HQMServerPlayerIndex>)>,
    puck_touches: HashMap<HQMObjectIndex, VecDeque<HQMPuckTouch>>,

    pub verified_players: HashMap<HQMServerPlayerIndex, i32>,
    pub queued_players: Vec<RHQMQueuePlayer>,
    pub status: State,

    pub rhqm_game: RHQMGame,

    pub(crate) pool: Pool,
    pub(crate) sender: crossbeam_channel::Sender<PostgresResponse>,
    pub(crate) receiver: crossbeam_channel::Receiver<PostgresResponse>,
}

impl HQMRanked {
    pub fn new(config: HQMRankedConfiguration, pool: Pool) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();

        Self {
            config,
            paused: true,
            pause_timer: 0,
            is_pause_goal: false,
            next_faceoff_spot: HQMRinkFaceoffSpot::Center,
            icing_status: HQMIcingStatus::No,
            offside_status: HQMOffsideStatus::Neutral,
            twoline_pass_status: HQMTwoLinePassStatus::No,
            pass: None,
            preferred_positions: HashMap::new(),
            started_as_goalie: vec![],
            faceoff_game_step: 0,
            too_late_printed_this_period: false,
            step_where_period_ended: 0,
            start_next_replay: None,
            puck_touches: Default::default(),

            verified_players: Default::default(),
            queued_players: vec![],
            status: State::Waiting {
                waiting_for_response: false,
            },
            rhqm_game: RHQMGame::new(),
            pool,
            sender,
            receiver,
        }
    }

    fn get_last_touch(&self, puck_index: HQMObjectIndex) -> Option<&HQMPuckTouch> {
        self.puck_touches.get(&puck_index).and_then(|x| x.front())
    }

    pub fn clear_started_goalie(&mut self, player_index: HQMServerPlayerIndex) {
        if let Some(x) = self
            .started_as_goalie
            .iter()
            .position(|x| *x == player_index)
        {
            self.started_as_goalie.remove(x);
        }
    }

    fn do_faceoff(&mut self, server: &mut HQMServer) {
        let positions = get_faceoff_positions(
            &server.players,
            &self.preferred_positions,
            &server.game.world,
        );

        server.game.world.clear_pucks();
        self.puck_touches.clear();

        let next_faceoff_spot = server
            .game
            .world
            .rink
            .get_faceoff_spot(self.next_faceoff_spot)
            .clone();

        let puck_pos = next_faceoff_spot.center_position + &(1.5f32 * Vector3::y());

        server
            .game
            .world
            .create_puck_object(puck_pos, Rotation3::identity());

        self.started_as_goalie.clear();
        for (player_index, (team, faceoff_position)) in positions {
            let (player_position, player_rotation) = match team {
                HQMTeam::Red => next_faceoff_spot.red_player_positions[&faceoff_position].clone(),
                HQMTeam::Blue => next_faceoff_spot.blue_player_positions[&faceoff_position].clone(),
            };
            server.spawn_skater(player_index, team, player_position, player_rotation);
            if faceoff_position == "G" {
                self.started_as_goalie.push(player_index);
            }
        }

        let rink = &server.game.world.rink;
        self.icing_status = HQMIcingStatus::No;
        self.offside_status = if rink
            .red_lines_and_net
            .offensive_line
            .point_past_middle_of_line(&puck_pos)
        {
            HQMOffsideStatus::InOffensiveZone(HQMTeam::Red)
        } else if rink
            .blue_lines_and_net
            .offensive_line
            .point_past_middle_of_line(&puck_pos)
        {
            HQMOffsideStatus::InOffensiveZone(HQMTeam::Blue)
        } else {
            HQMOffsideStatus::Neutral
        };
        self.twoline_pass_status = HQMTwoLinePassStatus::No;
        self.pass = None;

        self.faceoff_game_step = server.game.game_step;
    }

    pub(crate) fn update_game_over(&mut self, server: &mut HQMServer) {
        let time_gameover = self.config.time_intermission * 100;
        let time_break = self.config.time_break * 100;

        let red_score = server.game.red_score;
        let blue_score = server.game.blue_score;
        let old_game_over = server.game.game_over;
        server.game.game_over =
            if server.game.period > self.config.periods && red_score != blue_score {
                true
            } else if self.config.mercy > 0
                && (red_score.saturating_sub(blue_score) >= self.config.mercy
                    || blue_score.saturating_sub(red_score) >= self.config.mercy)
            {
                true
            } else if self.config.first_to > 0
                && (red_score >= self.config.first_to || blue_score >= self.config.first_to)
            {
                true
            } else {
                false
            };
        if server.game.game_over && !old_game_over {
            self.pause_timer = self.pause_timer.max(time_gameover);
        } else if !server.game.game_over && old_game_over {
            self.pause_timer = self.pause_timer.max(time_break);
        }
    }

    fn call_goal(
        &mut self,
        server: &mut HQMServer,
        team: HQMTeam,
        puck_index: HQMObjectIndex,
    ) -> HQMRankedEvent {
        let time_break = self.config.time_break * 100;

        match team {
            HQMTeam::Red => {
                server.game.red_score += 1;
            }
            HQMTeam::Blue => {
                server.game.blue_score += 1;
            }
        };

        self.next_faceoff_spot = HQMRinkFaceoffSpot::Center;

        let (
            goal_scorer_index,
            assist_index,
            puck_speed_across_line,
            puck_speed_from_stick,
            last_touch,
        ) = if let Some(this_puck) = server.game.world.objects.get_puck_mut(puck_index) {
            let mut goal_scorer_index = None;
            let mut assist_index = None;
            let mut goal_scorer_first_touch = 0;
            let mut puck_speed_from_stick = None;
            let mut last_touch = None;
            let puck_speed_across_line = this_puck.body.linear_velocity.norm();
            if let Some(touches) = self.puck_touches.get(&puck_index) {
                last_touch = touches.front().map(|x| x.player_index);

                for touch in touches.iter() {
                    if goal_scorer_index.is_none() {
                        if touch.team == team {
                            goal_scorer_index = Some(touch.player_index);
                            goal_scorer_first_touch = touch.first_time;
                            puck_speed_from_stick = Some(touch.puck_speed);

                            let scoring_rhqm_player =
                                self.rhqm_game.get_player_by_index_mut(touch.player_index);

                            if let Some(scoring_rhqm_player) = scoring_rhqm_player {
                                scoring_rhqm_player.goals += 1;

                                let x = this_puck.body.linear_velocity.norm();

                                let speed = ((x * 100.0 * 3.6) * 100.0).round() / 100.0;

                                let event = GameEvent {
                                    player_id: scoring_rhqm_player.player_id,
                                    type_id: 1,
                                    speed,
                                    time: server.game.time,
                                    period: server.game.period,
                                };

                                self.rhqm_game.events.push(event);
                            }
                        }
                    } else {
                        if touch.team == team {
                            if Some(touch.player_index) == goal_scorer_index {
                                goal_scorer_first_touch = touch.first_time;
                            } else {
                                // This is the first player on the scoring team that touched it apart from the goal scorer
                                // If more than 10 seconds passed between the goal scorer's first touch
                                // and this last touch, it doesn't count as an assist

                                let diff = touch.last_time.saturating_sub(goal_scorer_first_touch);

                                if diff <= 1000 {
                                    assist_index = Some(touch.player_index)
                                }
                                break;
                            }
                        }
                    }
                }
            }

            (
                goal_scorer_index,
                assist_index,
                puck_speed_across_line,
                puck_speed_from_stick,
                last_touch,
            )
        } else {
            (None, None, 0.0, None, None)
        };

        server
            .messages
            .add_goal_message(team, goal_scorer_index, assist_index);

        fn convert(puck_speed: f32, use_mph: bool) -> (f32, &'static str) {
            if use_mph {
                (puck_speed * 100f32 * 2.23693, "mph")
            } else {
                (puck_speed * 100f32 * 3.6, "km/h")
            }
        }

        let (puck_speed_across_line_converted, puck_speed_unit) =
            convert(puck_speed_across_line, self.config.use_mph);

        let str1 = format!(
            "Goal scored, {:.1} {} across line",
            puck_speed_across_line_converted, puck_speed_unit
        );

        let str2 = if let Some(puck_speed_from_stick) = puck_speed_from_stick {
            let (puck_speed_converted, puck_speed_unit) =
                convert(puck_speed_from_stick, self.config.use_mph);
            format!(
                ", {:.1} {} from stick",
                puck_speed_converted, puck_speed_unit
            )
        } else {
            "".to_owned()
        };
        let s = format!("{}{}", str1, str2);

        server.messages.add_server_chat_message(s);

        if server.game.time < 1000 {
            let time = server.game.time;
            let seconds = time / 100;
            let centi = time % 100;

            let s = format!("{}.{:02} seconds left", seconds, centi);
            server.messages.add_server_chat_message(s);
        }

        self.pause_timer = time_break;
        self.is_pause_goal = true;

        self.update_game_over(server);

        let gamestep = server.game.game_step;

        if self.config.goal_replay {
            let force_view = goal_scorer_index.or(last_touch);
            self.start_next_replay = Some((
                self.faceoff_game_step.max(gamestep - 600),
                gamestep + 200,
                force_view,
            ));

            self.pause_timer = self.pause_timer.saturating_sub(800).max(400);
        }
        HQMRankedEvent::Goal {
            team,
            time: server.game.time,
            period: server.game.period,
            goal: goal_scorer_index,
            assist: assist_index,
            speed: puck_speed_from_stick,
            speed_across_line: puck_speed_across_line,
        }
    }

    fn handle_events_end_of_period(
        &mut self,
        server: &mut HQMServer,
        events: &[HQMSimulationEvent],
    ) {
        for event in events {
            if let HQMSimulationEvent::PuckEnteredNet { .. } = event {
                let time = server
                    .game
                    .game_step
                    .saturating_sub(self.step_where_period_ended);
                if time <= 300 && !self.too_late_printed_this_period {
                    let seconds = time / 100;
                    let centi = time % 100;
                    self.too_late_printed_this_period = true;
                    let s = format!("{}.{:02} seconds too late!", seconds, centi);

                    server.messages.add_server_chat_message(s);
                }
            }
        }
    }

    fn handle_puck_touch(
        &mut self,
        server: &mut HQMServer,
        player: HQMObjectIndex,
        puck_index: HQMObjectIndex,
    ) {
        if let Some((player_index, touching_team, _)) = server.players.get_from_object_index(player)
        {
            if let Some(puck) = server.game.world.objects.get_puck_mut(puck_index) {
                add_touch(
                    puck,
                    self.puck_touches.entry(puck_index),
                    player_index,
                    player,
                    touching_team,
                    server.game.time,
                );

                if let Some(rhqm_player) = self.rhqm_game.get_player_by_index_mut(player_index) {
                    rhqm_player.touches += 1;
                }

                let side = if puck.body.pos.x <= &server.game.world.rink.width / 2.0 {
                    HQMRinkSide::Left
                } else {
                    HQMRinkSide::Right
                };
                self.pass = Some(HQMPass {
                    team: touching_team,
                    side,
                    from: None,
                    player: player_index,
                });

                let other_team = touching_team.get_other_team();

                if let HQMOffsideStatus::Warning(team, side, position, i) = self.offside_status {
                    if team == touching_team {
                        let self_touch = player_index == i;

                        self.call_offside(server, touching_team, side, position, self_touch);
                        return;
                    }
                }
                if let HQMTwoLinePassStatus::Warning(team, side, position, ref i) =
                    self.twoline_pass_status
                {
                    if team == touching_team && i.contains(&player_index) {
                        self.call_twoline_pass(server, touching_team, side, position);
                        return;
                    } else {
                        self.twoline_pass_status = HQMTwoLinePassStatus::No;
                        server
                            .messages
                            .add_server_chat_message_str("Two-line pass waved off");
                    }
                }
                if let HQMIcingStatus::Warning(team, side) = self.icing_status {
                    if touching_team != team && !self.started_as_goalie.contains(&player_index) {
                        self.call_icing(server, other_team, side);
                    } else {
                        self.icing_status = HQMIcingStatus::No;
                        server
                            .messages
                            .add_server_chat_message_str("Icing waved off");
                    }
                }
            }
        }
    }

    fn handle_puck_entered_net(
        &mut self,
        server: &mut HQMServer,
        events: &mut Vec<HQMRankedEvent>,
        team: HQMTeam,
        puck: HQMObjectIndex,
    ) {
        match self.offside_status {
            HQMOffsideStatus::Warning(offside_team, side, position, _) if offside_team == team => {
                self.call_offside(server, team, side, position, false);
            }
            HQMOffsideStatus::Offside(_) => {}
            _ => {
                events.push(self.call_goal(server, team, puck));
            }
        }
    }

    fn handle_puck_passed_goal_line(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if let Some(HQMPass {
            team: icing_team,
            side,
            from: Some(transition),
            ..
        }) = self.pass
        {
            if team == icing_team && transition <= HQMPassPosition::ReachedCenter {
                match self.config.icing {
                    HQMIcingConfiguration::Touch => {
                        self.icing_status = HQMIcingStatus::Warning(team, side);
                        server.messages.add_server_chat_message_str("Icing warning");
                    }
                    HQMIcingConfiguration::NoTouch => {
                        self.call_icing(server, team, side);
                    }
                    HQMIcingConfiguration::Off => {}
                }
            }
        }
    }

    fn puck_into_offside_zone(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if self.offside_status == HQMOffsideStatus::InOffensiveZone(team) {
            return;
        }
        if let Some(HQMPass {
            team: pass_team,
            side,
            from: transition,
            player,
        }) = self.pass
        {
            if team == pass_team && has_players_in_offensive_zone(&server, team, Some(player)) {
                match self.config.offside {
                    HQMOffsideConfiguration::Delayed => {
                        self.offside_status =
                            HQMOffsideStatus::Warning(team, side, transition, player);
                        server
                            .messages
                            .add_server_chat_message_str("Offside warning");
                    }
                    HQMOffsideConfiguration::Immediate => {
                        self.call_offside(server, team, side, transition, false);
                    }
                    HQMOffsideConfiguration::Off => {
                        self.offside_status = HQMOffsideStatus::InOffensiveZone(team);
                    }
                }
            } else {
                self.offside_status = HQMOffsideStatus::InOffensiveZone(team);
            }
        } else {
            self.offside_status = HQMOffsideStatus::InOffensiveZone(team);
        }
    }

    fn handle_puck_entered_offensive_half(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if !matches!(&self.offside_status, HQMOffsideStatus::Offside(_))
            && self.config.offside_line == HQMOffsideLineConfiguration::Center
        {
            self.puck_into_offside_zone(server, team);
        }
        if let HQMOffsideStatus::Warning(warning_team, _, _, _) = self.offside_status {
            if warning_team != team {
                server
                    .messages
                    .add_server_chat_message_str("Offside waved off");
            }
        }
        if let Some(HQMPass {
            team: pass_team,
            side,
            from: Some(from),
            player: pass_player,
        }) = self.pass
        {
            if self.twoline_pass_status == HQMTwoLinePassStatus::No && pass_team == team {
                let is_regular_twoline_pass_active = self.config.twoline_pass
                    == HQMTwoLinePassConfiguration::Double
                    || self.config.twoline_pass == HQMTwoLinePassConfiguration::On;
                if from <= HQMPassPosition::ReachedOwnBlue && is_regular_twoline_pass_active {
                    self.check_twoline_pass(server, team, side, from, pass_player, false);
                }
            }
        }
    }

    fn handle_puck_entered_offensive_zone(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if !matches!(&self.offside_status, HQMOffsideStatus::Offside(_))
            && self.config.offside_line == HQMOffsideLineConfiguration::OffensiveBlue
        {
            self.puck_into_offside_zone(server, team);
        }
        if let Some(HQMPass {
            team: pass_team,
            side,
            from: Some(from),
            player: pass_player,
        }) = self.pass
        {
            if self.twoline_pass_status == HQMTwoLinePassStatus::No && pass_team == team {
                let is_forward_twoline_pass_active = self.config.twoline_pass
                    == HQMTwoLinePassConfiguration::Double
                    || self.config.twoline_pass == HQMTwoLinePassConfiguration::Forward;
                let is_threeline_pass_active =
                    self.config.twoline_pass == HQMTwoLinePassConfiguration::ThreeLine;
                if (from <= HQMPassPosition::ReachedCenter && is_forward_twoline_pass_active)
                    || from <= HQMPassPosition::ReachedOwnBlue && is_threeline_pass_active
                {
                    self.check_twoline_pass(server, team, side, from, pass_player, true);
                }
            }
        }
    }

    fn check_twoline_pass(
        &mut self,
        server: &mut HQMServer,
        team: HQMTeam,
        side: HQMRinkSide,
        from: HQMPassPosition,
        pass_player: HQMServerPlayerIndex,
        is_offensive_line: bool,
    ) {
        let team_line = match team {
            HQMTeam::Red => &server.game.world.rink.red_lines_and_net,
            HQMTeam::Blue => &server.game.world.rink.blue_lines_and_net,
        };
        let line = if is_offensive_line {
            &team_line.offensive_line
        } else {
            &team_line.mid_line
        };
        let mut players_past_line = vec![];
        for (player_index, player) in server.players.iter() {
            if player_index == pass_player {
                continue;
            }
            if let Some(player) = player {
                if is_past_line(server, player, team, line) {
                    players_past_line.push(player_index);
                }
            }
        }
        if !players_past_line.is_empty() {
            self.twoline_pass_status =
                HQMTwoLinePassStatus::Warning(team, side, from, players_past_line);
            server
                .messages
                .add_server_chat_message_str("Two-line pass warning");
        }
    }

    fn handle_puck_passed_defensive_line(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if !matches!(&self.offside_status, HQMOffsideStatus::Offside(_))
            && self.config.offside_line == HQMOffsideLineConfiguration::OffensiveBlue
        {
            if let HQMOffsideStatus::Warning(t, _, _, _) = self.offside_status {
                if team.get_other_team() == t {
                    server
                        .messages
                        .add_server_chat_message_str("Offside waved off");
                }
            }
            self.offside_status = HQMOffsideStatus::Neutral;
        }
    }

    fn update_pass(&mut self, team: HQMTeam, p: HQMPassPosition) {
        if let Some(pass) = &mut self.pass {
            if pass.team == team && pass.from.is_none() {
                pass.from = Some(p);
            }
        }
    }

    fn check_wave_off_twoline(&mut self, server: &mut HQMServer, team: HQMTeam) {
        if let HQMTwoLinePassStatus::Warning(warning_team, _, _, _) = self.twoline_pass_status {
            if team != warning_team {
                self.twoline_pass_status = HQMTwoLinePassStatus::No;
                server
                    .messages
                    .add_server_chat_message_str("Two-line pass waved off");
            }
        }
    }

    fn handle_events(
        &mut self,
        server: &mut HQMServer,
        events: &[HQMSimulationEvent],
        match_events: &mut Vec<HQMRankedEvent>,
    ) {
        for event in events {
            match *event {
                HQMSimulationEvent::PuckEnteredNet { team, puck } => {
                    self.handle_puck_entered_net(server, match_events, team, puck);
                }
                HQMSimulationEvent::PuckTouch { player, puck, .. } => {
                    self.handle_puck_touch(server, player, puck);
                }
                HQMSimulationEvent::PuckReachedDefensiveLine { team, puck: _ } => {
                    self.check_wave_off_twoline(server, team);
                    self.update_pass(team, HQMPassPosition::ReachedOwnBlue);
                }
                HQMSimulationEvent::PuckPassedDefensiveLine { team, puck: _ } => {
                    self.update_pass(team, HQMPassPosition::PassedOwnBlue);
                    self.handle_puck_passed_defensive_line(server, team);
                }
                HQMSimulationEvent::PuckReachedCenterLine { team, puck: _ } => {
                    self.check_wave_off_twoline(server, team);
                    self.update_pass(team, HQMPassPosition::ReachedCenter);
                }
                HQMSimulationEvent::PuckPassedCenterLine { team, puck: _ } => {
                    self.update_pass(team, HQMPassPosition::PassedCenter);
                    self.handle_puck_entered_offensive_half(server, team);
                }
                HQMSimulationEvent::PuckReachedOffensiveZone { team, puck: _ } => {
                    self.update_pass(team, HQMPassPosition::ReachedOffensive);
                }
                HQMSimulationEvent::PuckEnteredOffensiveZone { team, puck: _ } => {
                    self.update_pass(team, HQMPassPosition::PassedOffensive);
                    self.handle_puck_entered_offensive_zone(server, team);
                }
                HQMSimulationEvent::PuckPassedGoalLine { team, puck: _ } => {
                    self.handle_puck_passed_goal_line(server, team);
                }
                _ => {}
            }

            if self.pause_timer > 0
                || server.game.time == 0
                || server.game.game_over
                || server.game.period == 0
            {
                return;
            }
        }
    }

    fn call_offside(
        &mut self,
        server: &mut HQMServer,
        team: HQMTeam,
        side: HQMRinkSide,
        position: Option<HQMPassPosition>,
        self_touch: bool,
    ) {
        let time_break = self.config.time_break * 100;

        let faceoff_spot = if self_touch {
            match self.config.offside_line {
                HQMOffsideLineConfiguration::OffensiveBlue => {
                    HQMRinkFaceoffSpot::Offside(team.get_other_team(), side)
                }
                HQMOffsideLineConfiguration::Center => HQMRinkFaceoffSpot::Center,
            }
        } else {
            match position {
                Some(p) if p <= HQMPassPosition::ReachedOwnBlue => {
                    HQMRinkFaceoffSpot::DefensiveZone(team, side)
                }
                Some(p) if p <= HQMPassPosition::ReachedCenter => {
                    HQMRinkFaceoffSpot::Offside(team, side)
                }
                _ => HQMRinkFaceoffSpot::Center,
            }
        };

        self.next_faceoff_spot = faceoff_spot;
        self.pause_timer = time_break;
        self.offside_status = HQMOffsideStatus::Offside(team);
        server.messages.add_server_chat_message_str("Offside");
    }

    fn call_twoline_pass(
        &mut self,
        server: &mut HQMServer,
        team: HQMTeam,
        side: HQMRinkSide,
        position: HQMPassPosition,
    ) {
        let time_break = self.config.time_break * 100;

        let faceoff_spot = if position <= HQMPassPosition::ReachedOwnBlue {
            HQMRinkFaceoffSpot::DefensiveZone(team, side)
        } else if position <= HQMPassPosition::ReachedCenter {
            HQMRinkFaceoffSpot::Offside(team, side)
        } else {
            HQMRinkFaceoffSpot::Center
        };

        self.next_faceoff_spot = faceoff_spot;
        self.pause_timer = time_break;
        self.twoline_pass_status = HQMTwoLinePassStatus::Offside(team);
        server.messages.add_server_chat_message_str("Two-line pass");
    }

    fn call_icing(&mut self, server: &mut HQMServer, team: HQMTeam, side: HQMRinkSide) {
        let time_break = self.config.time_break * 100;

        self.next_faceoff_spot = HQMRinkFaceoffSpot::DefensiveZone(team, side);
        self.pause_timer = time_break;
        self.icing_status = HQMIcingStatus::Icing(team);
        server.messages.add_server_chat_message_str("Icing");
    }

    pub fn after_tick(
        &mut self,
        server: &mut HQMServer,
        events: &[HQMSimulationEvent],
    ) -> Vec<HQMRankedEvent> {
        let mut match_events = vec![];
        if server.game.time == 0 && server.game.period > 1 {
            self.handle_events_end_of_period(server, events);
        } else if self.pause_timer > 0
            || server.game.time == 0
            || server.game.game_over
            || server.game.period == 0
            || self.paused
        {
            // Nothing
        } else {
            self.handle_events(server, events, &mut match_events);

            if let HQMOffsideStatus::Warning(team, _, _, _) = self.offside_status {
                if !has_players_in_offensive_zone(server, team, None) {
                    self.offside_status = HQMOffsideStatus::InOffensiveZone(team);
                    server
                        .messages
                        .add_server_chat_message_str("Offside waved off");
                }
            }

            let rules_state = if matches!(self.offside_status, HQMOffsideStatus::Offside(_))
                || matches!(self.twoline_pass_status, HQMTwoLinePassStatus::Offside(_))
            {
                HQMRulesState::Offside
            } else if matches!(self.icing_status, HQMIcingStatus::Icing(_)) {
                HQMRulesState::Icing
            } else {
                let icing_warning = matches!(self.icing_status, HQMIcingStatus::Warning(_, _));
                let offside_warning =
                    matches!(self.offside_status, HQMOffsideStatus::Warning(_, _, _, _))
                        || matches!(
                            self.twoline_pass_status,
                            HQMTwoLinePassStatus::Warning(_, _, _, _)
                        );
                HQMRulesState::Regular {
                    offside_warning,
                    icing_warning,
                }
            };

            server.game.rules_state = rules_state;
        }

        self.update_clock(server);

        if let Some((start_replay, end_replay, force_view)) = self.start_next_replay {
            if end_replay <= server.game.game_step {
                server.add_replay_to_queue(start_replay, end_replay, force_view);
                server.messages.add_server_chat_message_str("Goal replay");
                self.start_next_replay = None;
            }
        }
        match_events
    }

    fn update_clock(&mut self, server: &mut HQMServer) {
        let period_length = self.config.time_period * 100;
        let intermission_time = self.config.time_intermission * 100;

        if !self.paused {
            if self.pause_timer > 0 {
                self.pause_timer -= 1;
                if self.pause_timer == 0 {
                    self.is_pause_goal = false;
                    if server.game.game_over {
                        server.new_game(self.create_game());
                    } else {
                        if server.game.time == 0 {
                            server.game.time = period_length;
                        }

                        self.do_faceoff(server);
                    }
                }
            } else {
                server.game.time = server.game.time.saturating_sub(1);
                if server.game.time == 0 {
                    server.game.period += 1;
                    self.pause_timer = intermission_time;
                    self.is_pause_goal = false;
                    self.step_where_period_ended = server.game.game_step;
                    self.too_late_printed_this_period = false;
                    self.next_faceoff_spot = HQMRinkFaceoffSpot::Center;
                    self.update_game_over(server);
                }
            }
        }
        server.game.goal_message_timer = if self.is_pause_goal {
            self.pause_timer
        } else {
            0
        };
    }

    pub fn cleanup_player(&mut self, player_index: HQMServerPlayerIndex) {
        if let Some(x) = self
            .started_as_goalie
            .iter()
            .position(|x| *x == player_index)
        {
            self.started_as_goalie.remove(x);
        }
        self.preferred_positions.remove(&player_index);
    }

    pub fn create_game(&mut self) -> HQMGame {
        self.status = State::Waiting {
            waiting_for_response: false,
        };
        self.puck_touches.clear();
        self.rhqm_game = RHQMGame::new();
        self.paused = true;
        self.pause_timer = 0;
        self.next_faceoff_spot = HQMRinkFaceoffSpot::Center;
        self.icing_status = HQMIcingStatus::No;
        self.offside_status = HQMOffsideStatus::Neutral;
        self.twoline_pass_status = HQMTwoLinePassStatus::No;
        self.start_next_replay = None;

        let warmup_pucks = self.config.warmup_pucks;

        let mut game = HQMGame::new(
            warmup_pucks,
            self.config.physics_config.clone(),
            self.config.blue_line_location,
        );
        let puck_line_start = game.world.rink.width / 2.0 - 0.4 * ((warmup_pucks - 1) as f32);

        for i in 0..warmup_pucks {
            let pos = Point3::new(
                puck_line_start + 0.8 * (i as f32),
                1.5,
                game.world.rink.length / 2.0,
            );
            let rot = Rotation3::identity();
            game.world.create_puck_object(pos, rot);
        }
        game.time = self.config.time_warmup * 100;
        game
    }

    pub fn onethree(&mut self, server: &mut HQMServer, player_index: HQMServerPlayerIndex) {
        if matches!(self.status, State::Game { .. }) {
            let player = server.players.get(player_index);
            let team = if let Some(team) = player.and_then(|x| x.object).map(|x| x.1) {
                team
            } else {
                return;
            };

            let mut players_in_team = SmallVec::<[HQMServerPlayerIndex; 8]>::new();

            for (player_index, player) in server.players.iter() {
                let player_team = player.and_then(|x| x.object).map(|x| x.1);
                if player_team == Some(team) {
                    players_in_team.push(player_index);
                }
            }

            let goalie = match team {
                HQMTeam::Red => &self.rhqm_game.red_goalie,
                HQMTeam::Blue => &self.rhqm_game.blue_goalie,
            };

            match goalie {
                RHQMGameGoalie::TakeTurns {
                    first,
                    second,
                    third,
                    overtime,
                } => {
                    let msg1 = format!("[Server] G 1st: {}, 2nd: {}", limit(first), limit(second));
                    let msg2 = format!("[Server] G 3rd: {}, OT: {}", limit(third), limit(overtime));
                    for player_index in players_in_team.iter() {
                        server
                            .messages
                            .add_directed_server_chat_message(msg1.clone(), *player_index);
                        server
                            .messages
                            .add_directed_server_chat_message(msg2.clone(), *player_index);
                    }
                }
                RHQMGameGoalie::Fixed { goalie } => {
                    let msg = format!("[Server] G: {}", goalie);
                    for player_index in players_in_team.iter() {
                        server
                            .messages
                            .add_directed_server_chat_message(msg.clone(), *player_index);
                    }
                }
                RHQMGameGoalie::Undefined => {}
            };
        }
    }

    pub fn iamg(&mut self, server: &mut HQMServer, player_index: HQMServerPlayerIndex) {
        if matches!(self.status, State::Game { .. }) {
            let player = self.rhqm_game.get_player_by_index(player_index);
            let team = if let Some(team) = player.and_then(|x| x.player_team) {
                team
            } else {
                return;
            };

            let mut players_in_team = SmallVec::<[HQMServerPlayerIndex; 8]>::new();

            for player_item in self.rhqm_game.game_players.iter() {
                if let Some(player_index) = player_item.player_index {
                    if Some(team) == player_item.player_team {
                        players_in_team.push(player_index);
                    }
                }
            }

            if let Some(player) = self.rhqm_game.get_player_by_index(player_index) {
                let name = player.player_name.clone();
                let msg = format!("[Server] {} will be goalie for this game!", name);
                let goalie = match team {
                    HQMTeam::Red => &mut self.rhqm_game.red_goalie,
                    HQMTeam::Blue => &mut self.rhqm_game.blue_goalie,
                };
                *goalie = RHQMGameGoalie::Fixed { goalie: name };

                for player_index in players_in_team.iter() {
                    server
                        .messages
                        .add_directed_server_chat_message(msg.clone(), *player_index);
                }
            }
        }
    }

    pub fn login(
        &mut self,
        server: &mut HQMServer,
        player_index: HQMServerPlayerIndex,
        password_user: &str,
    ) {
        if let Some(player) = server.players.get(player_index) {
            let name = player.player_name.to_string();
            let rhqm_player = self.rhqm_game.get_player_by_index(player_index);
            let is_on_ice = player.object.is_some();
            let queued = self.queued_players.iter().any(|x| &x.player_name == &name);
            if queued || (rhqm_player.is_some() && is_on_ice) {
                server.messages.add_directed_server_chat_message_str(
                    "You are already logged in",
                    player_index,
                );
            } else {
                let already_verified = self.verified_players.get(&player_index).copied();
                let pass_str = password_user.to_string();

                fn get_hash(pass_str: &str) -> String {
                    format!("{:X}", md5::compute(pass_str))
                }

                let username = (*player.player_name).clone();
                let pool = self.pool.clone();
                let sender = self.sender.clone();

                tokio::spawn(async move {
                    let client = pool.get().await?;

                    let sql1 =
                        "SELECT u.\"Id\", u.\"Password\", COALESCE(nu.\"From\",'') FROM public.\"Users\" u
                        left join public.\"NicknameUsings\" nu on nu.\"PlayerId\"=u.\"Id\" and \"Date\">(NOW() - interval '1 month')
                        where \"Login\"=$1 limit 1";
                    let sql2 = "select checkban($1)";

                    let query1 = client.query(sql1, &[&username]).await?;
                    if query1.len() == 1 {
                        let row = &query1[0];
                        let player_id: i32 = row.get(0);
                        let password: String = row.get(1);
                        let old_nickname: String = row.get(2);
                        if Some(player_id) == already_verified || password == get_hash(&pass_str) {
                            let query2 = &client.query(sql2, &[&username]).await?[0];
                            if query2.get::<_, i32>(0) > 0 {
                                sender.send(PostgresResponse::LoginBanned { player_index })?;
                            } else {
                                sender.send(PostgresResponse::LoginSuccessful {
                                    player_id,
                                    player_index,
                                    old_nickname,
                                })?;
                            }
                        } else {
                            sender.send(PostgresResponse::LoginFailed { player_index })?;
                        }
                    }
                    Ok::<_, anyhow::Error>(())
                });
            }
        }
    }

    pub(crate) fn get_player_and_ips(
        &self,
        server: &HQMServer,
        players: &[RHQMQueuePlayer],
    ) -> (Vec<String>, Vec<String>, Vec<i32>) {
        players
            .iter()
            .map(|x| {
                let name = x.player_name.clone();
                let ip = server
                    .players
                    .get(x.player_index)
                    .and_then(|player| player.addr().map(|x| x.ip()));
                let ip = ip.map(|x| x.to_string()).unwrap_or("".to_owned());

                (name, ip, x.player_id)
            })
            .multiunzip()
    }

    pub(crate) fn send_notify(
        &mut self,
        players: Vec<String>,
        ips: Vec<String>,
        server_name: String,
    ) {
        self.rhqm_game.need_to_send = false;
        let players_count = players.len();

        let pool = self.pool.clone();
        if self.config.notification == true && players_count >= 5 {
            tokio::spawn(async move {
                let baseurl = "https://api.hqmranked.ru:5000/api/telegram/notif".to_string();

                let client = reqwest::Client::new();
                let mut map = HashMap::new();
                let text = format!(
                    "{} [{} players]\n{}",
                    server_name,
                    players_count,
                    players.join("\n")
                );
                map.insert("text", text);

                if let Err(err) = client.post(baseurl).json(&map).send().await {
                    eprintln!("Error sending notification: {:?}", err);
                }
            });
        }
    }

    pub(crate) fn request_player_points_and_win_rate(&self, player_ids: Vec<i32>) {
        let pool = self.pool.clone();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            let client = pool.get().await.unwrap();
            let mut res = HashMap::new();
            for id in player_ids {
                let point_sql = "select COALESCE(sum(\"Score\"),0) from public.\"GameStats\" where \"GameId\" in (select \"Id\" from public.\"Stats\" where \"Season\"=(select currentseason()))
            and \"Player\" = $1";
                let winrate_sql = "select \"Winrate\" from public.\"PlayerDatas\"
            where \"PlayerId\" = $1 and \"Season\" = currentseason()";

                let baseurl = "https://api.hqmranked.ru:5000/api/game/getelo".to_string();

                let clientr = reqwest::Client::new();
                let mut map = HashMap::new();
                let player_id_slice = &[&id];
                map.insert("playerID", player_id_slice);

                let resr = clientr.post(baseurl).json(&map).send().await?;

                let resr_text = resr.text().await?;
                let resr_int = resr_text.parse::<i32>().unwrap();

                //     let rating_sql = "select round(100/(prMax.\"Mu\" -prMin.\"Mu\")*(pr.\"Mu\" - prMin.\"Mu\"))::integer from public.\"PlayerRatings\" pr
                // left join public.\"PlayerRatings\" prMax on prMax.\"Mu\"=(select max(\"Mu\") from public.\"PlayerRatings\")
                // left join public.\"PlayerRatings\" prMin on prMin.\"Mu\"=(select min(\"Mu\") from public.\"PlayerRatings\")
                // where pr.\"PlayerId\"=$1";
                let point = client.query(point_sql, &[&id]).await?;
                let win_rate_res = client.query(winrate_sql, &[&id]).await?;
                let win_rate_res = win_rate_res.get(0);
                // let rating_res = client.query(rating_sql, &[&id]).await?;

                let point: i64 = point.get(0).map(|x| x.get(0)).unwrap_or(0);
                let win_rate: f64 = win_rate_res.map(|x| x.get(0)).unwrap_or(0.0);
                // let mut rating: i32 = 0i32;
                // if rating_res.len() > 0 {
                //     rating = rating_res.get(0).map(|x| x.get(0)).unwrap_or(0);
                // }

                res.insert(id, (point, win_rate, resr_int));
            }
            sender.send(PostgresResponse::PlayerData { data: res })?;
            Ok::<_, anyhow::Error>(())
        });
    }

    fn force_players_off_ice_by_system(&mut self, server: &mut HQMServer) {
        for i in 0..server.players.len() {
            server.move_to_spectator(HQMServerPlayerIndex(i));
        }
    }

    pub fn pick(
        &mut self,
        server: &mut HQMServer,
        sending_player: HQMServerPlayerIndex,
        arg: &str,
    ) {
        if let State::CaptainsPicking {
            ref options,
            current_team,
            ..
        } = self.status
        {
            if let Some(picking_rhqm_player) = self.rhqm_game.get_player_by_index(sending_player) {
                let picking_rhqm_id = picking_rhqm_player.player_id;

                let (current_picking_captain, other_picking_captain) = match current_team {
                    HQMTeam::Red => (self.rhqm_game.red_captain, self.rhqm_game.blue_captain),
                    HQMTeam::Blue => (self.rhqm_game.blue_captain, self.rhqm_game.red_captain),
                };
                if current_picking_captain == Some(picking_rhqm_id) {
                    let p = options
                        .iter()
                        .find(|(a, _, _, _)| a.eq_ignore_ascii_case(arg))
                        .cloned();
                    if let Some((_, _, player_id, _)) = p {
                        self.make_pick(server, player_id);
                    } else {
                        server.messages.add_directed_server_chat_message_str(
                            "[Server] Invalid pick",
                            sending_player,
                        );
                    }
                } else if other_picking_captain == Some(picking_rhqm_id) {
                    server.messages.add_directed_server_chat_message_str(
                        "[Server] It's not your turn to pick",
                        sending_player,
                    );
                } else {
                    server.messages.add_directed_server_chat_message_str(
                        "[Server] You're not a captain",
                        sending_player,
                    );
                }
            }
        }
    }

    fn update_status_after_pick(&mut self, server: &mut HQMServer, player_id: i32) {
        if let State::CaptainsPicking {
            ref mut options,
            current_team,
            ..
        } = self.status
        {
            let pick_order = [HQMTeam::Blue, HQMTeam::Blue, HQMTeam::Red, HQMTeam::Red];
            let pick_order_len = pick_order.len();
            self.rhqm_game.pick_number = self.rhqm_game.pick_number + 1;
            let cu_pick = (self.rhqm_game.pick_number - 1) % pick_order_len;

            // Find out how many players there are left
            let remaining_players = self
                .rhqm_game
                .game_players
                .iter()
                .filter(|p| p.player_team.is_none())
                .map(|p| p.player_id)
                .collect::<Vec<_>>();
            if remaining_players.is_empty() {
                // Weird, should not happen
                self.start_captains_game(server);
            } else if remaining_players.len() == 1 {
                let other_team = pick_order[cu_pick];
                let remaining_player = self
                    .rhqm_game
                    .get_player_by_id_mut(remaining_players[0])
                    .unwrap();
                remaining_player.player_team = Some(other_team);
                if let Some(player_index) = remaining_player.player_index {
                    server.spawn_skater_at_spawnpoint(
                        player_index,
                        other_team,
                        HQMSpawnPoint::Bench,
                    );
                }
                self.start_captains_game(server);
            } else {
                let mut old_options = std::mem::replace(options, vec![]);
                old_options.retain(|(_, _, id, _)| *id != player_id);
                server.game.period = 0;
                server.game.time = 2000;
                self.status = State::CaptainsPicking {
                    time_left: 2000,
                    options: old_options,
                    current_team: pick_order[cu_pick],
                };
                self.send_available_picks(server);
            }
        }
    }

    fn start_captains_game(&mut self, server: &mut HQMServer) {
        server.game.time = 2000;
        self.status = State::Game { paused: false };

        let mut red_player_names = SmallVec::<[_; 8]>::new();
        let mut blue_player_names = SmallVec::<[_; 8]>::new();
        for p in self.rhqm_game.game_players.iter() {
            match p.player_team {
                Some(HQMTeam::Red) => red_player_names.push(p.player_name.clone()),
                Some(HQMTeam::Blue) => blue_player_names.push(p.player_name.clone()),
                _ => {}
            };
        }
        let mut rand = rand::thread_rng();
        red_player_names.shuffle(&mut rand);
        blue_player_names.shuffle(&mut rand);
        let mut red_iter = red_player_names.into_iter().cycle();
        let mut blue_iter = blue_player_names.into_iter().cycle();
        self.rhqm_game.red_goalie = RHQMGameGoalie::TakeTurns {
            first: red_iter.next().unwrap(),
            second: red_iter.next().unwrap(),
            third: red_iter.next().unwrap(),
            overtime: red_iter.next().unwrap(),
        };
        self.rhqm_game.blue_goalie = RHQMGameGoalie::TakeTurns {
            first: blue_iter.next().unwrap(),
            second: blue_iter.next().unwrap(),
            third: blue_iter.next().unwrap(),
            overtime: blue_iter.next().unwrap(),
        };
    }

    pub(crate) fn make_default_pick(&mut self, server: &mut HQMServer) -> (String, i32) {
        if let State::CaptainsPicking { current_team, .. } = self.status {
            let best_remaining = self
                .rhqm_game
                .game_players
                .iter_mut()
                .filter(|p| p.player_team.is_none())
                .max_by_key(|x| x.rating)
                .unwrap();
            let player_id = best_remaining.player_id;
            let player_name = best_remaining.player_name.clone();
            best_remaining.player_team = Some(current_team);
            if let Some(player_index) = best_remaining.player_index {
                server.spawn_skater_at_spawnpoint(player_index, current_team, HQMSpawnPoint::Bench);
            }
            let msg = format!(
                "[Server] Time ran out, {} has been picked for {}",
                player_name, current_team
            );
            server.messages.add_server_chat_message(msg);
            self.update_status_after_pick(server, player_id);
            (player_name, player_id)
        } else {
            panic!();
        }
    }

    fn make_pick(&mut self, server: &mut HQMServer, player_id: i32) -> bool {
        if let State::CaptainsPicking { current_team, .. } = self.status {
            if let Some(picked_player) = self.rhqm_game.get_player_by_id_mut(player_id) {
                if picked_player.player_team.is_none() {
                    picked_player.player_team = Some(current_team);
                    let msg = format!(
                        "[Server] {} has been picked for {}",
                        picked_player.player_name, current_team
                    );
                    server.messages.add_server_chat_message(msg);
                    if let Some(player_index) = picked_player.player_index {
                        server.spawn_skater_at_spawnpoint(
                            player_index,
                            current_team,
                            HQMSpawnPoint::Bench,
                        );
                    }
                    self.update_status_after_pick(server, player_id);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn send_available_picks_command(
        &self,
        server: &mut HQMServer,
        player_index: HQMServerPlayerIndex,
    ) {
        if let State::CaptainsPicking { ref options, .. } = self.status {
            let mut msgs = SmallVec::<[_; 16]>::new();
            let iter = options.iter().chunks(3);
            for row in &iter {
                let s = row
                    .map(|(code, name, _, rating)| {
                        format!("{}: {} ({})", code, limit(name), rating)
                    })
                    .join(", ");
                msgs.push(s);
            }
            for msg in msgs.iter() {
                server
                    .messages
                    .add_directed_server_chat_message(msg.clone(), player_index);
            }
        }
    }

    fn send_available_picks(&self, server: &mut HQMServer) {
        if let State::CaptainsPicking {
            time_left: _,
            ref options,
            current_team,
        } = self.status
        {
            let current_captain = match current_team {
                HQMTeam::Red => self.rhqm_game.red_captain,
                HQMTeam::Blue => self.rhqm_game.blue_captain,
            }
            .and_then(|current_captain| self.rhqm_game.get_player_by_id(current_captain))
            .expect("captain value is invalid");

            let captain_name = current_captain.player_name.clone();
            let mut msgs = SmallVec::<[_; 16]>::new();
            let iter = options.iter().chunks(3);
            for row in &iter {
                let s = row
                    .map(|(code, name, _, rating)| {
                        format!("{}: {} ({})", code, limit(name), rating)
                    })
                    .join(", ");
                msgs.push(s);
            }
            let captain_player_index = current_captain.player_index;
            if let Some(captain_player_index) = captain_player_index {
                server.messages.add_directed_server_chat_message_str(
                    "It's your turn to pick! /p X",
                    captain_player_index,
                );
                for msg in msgs.iter() {
                    server
                        .messages
                        .add_directed_server_chat_message(msg.clone(), captain_player_index);
                }
            }
            let other_player_indices = server
                .players
                .iter()
                .filter(|(a, b)| b.is_some() && Some(*a) != captain_player_index)
                .map(|(a, _)| a)
                .collect::<Vec<_>>();
            for player_index in other_player_indices {
                let msg1 = format!(
                    "It's {}'s turn to pick for team {}!",
                    limit(&captain_name),
                    current_team
                );
                server
                    .messages
                    .add_directed_server_chat_message(msg1, player_index);

                for msg in msgs.iter() {
                    server
                        .messages
                        .add_directed_server_chat_message(msg.clone(), player_index);
                }
            }
        }
    }

    fn set_captains(&mut self, server: &mut HQMServer) -> Vec<(String, String, i32, i32)> {
        let middle = server.game.world.rink.length / 2.0;

        self.rhqm_game
            .game_players
            .sort_by(|a, b| b.rating.cmp(&a.rating));

        let red_captain = &mut self.rhqm_game.game_players[1];
        let red_captain_name = red_captain.player_name.clone();
        red_captain.player_team = Some(HQMTeam::Red);
        self.rhqm_game.red_captain = Some(red_captain.player_id);
        if let Some(red_captain_index) = red_captain.player_index {
            let pos = Point3::new(0.5, 2.0, middle + 3.0);
            let rot = Rotation3::from_euler_angles(0.0, 3.0 * FRAC_PI_2, 0.0);
            server.spawn_skater(red_captain_index, HQMTeam::Red, pos, rot);
        }

        let blue_captain = &mut self.rhqm_game.game_players[0];
        let blue_captain_name = blue_captain.player_name.clone();
        blue_captain.player_team = Some(HQMTeam::Blue);
        self.rhqm_game.blue_captain = Some(blue_captain.player_id);
        if let Some(blue_captain_index) = blue_captain.player_index {
            let pos = Point3::new(0.5, 2.0, middle - 3.0);
            let rot = Rotation3::from_euler_angles(0.0, 3.0 * FRAC_PI_2, 0.0);
            server.spawn_skater(blue_captain_index, HQMTeam::Blue, pos, rot);
        }

        let mut options = vec![];

        for (player, c) in self.rhqm_game.game_players[2..]
            .iter()
            .zip("ABCDEFGHIJKLMNOPQRSTUVWXYZ".chars())
        {
            options.push((
                c.to_string(),
                player.player_name.clone(),
                player.player_id,
                player.rating,
            ));
        }

        let msg = format!(
            "[Server] Red ({}) vs Blue ({})",
            limit(&red_captain_name),
            limit(&blue_captain_name)
        );

        server.messages.add_server_chat_message(msg);

        return options;
    }

    fn set_teams_by_server(&mut self, server: &mut HQMServer, sum: f64) {
        let middle = server.game.world.rink.length / 2.0;

        let mut sum_red = 0.0;
        let mut sum_blue = 0.0;
        let half_sum = sum / 2.0;

        let mut red_count = 0;
        let mut blue_count = 0;

        self.rhqm_game
            .game_players
            .sort_by(|a, b| b.player_points.partial_cmp(&a.player_points).unwrap());

        let mut red_player_names = SmallVec::<[_; 8]>::new();
        let mut blue_player_names = SmallVec::<[_; 8]>::new();

        let mut spawns: SmallVec<[(Point3<f32>, HQMTeam, HQMServerPlayerIndex); 16]> =
            SmallVec::new();

        for i in self.rhqm_game.game_players.iter_mut() {
            match i {
                RHQMGamePlayer {
                    player_index,
                    player_name,
                    winrate,
                    player_team,
                    ..
                } => {
                    let winrate = *winrate;
                    let team = if red_count == self.config.team_max {
                        HQMTeam::Blue
                    } else if blue_count == self.config.team_max {
                        HQMTeam::Red
                    } else if sum_red <= sum_blue || sum_blue >= half_sum {
                        HQMTeam::Red
                    } else {
                        HQMTeam::Blue
                    };
                    *player_team = Some(team);
                    let extra = if team == HQMTeam::Red {
                        sum_red = sum_red + winrate;
                        red_player_names.push(player_name.clone());
                        red_count += 1;
                        (red_count - 1) as f32
                    } else {
                        sum_blue = sum_blue + winrate;
                        blue_player_names.push(player_name.clone());
                        blue_count += 1;
                        (blue_count - 1) as f32
                    };
                    if let Some(player_index) = *player_index {
                        let z = if team == HQMTeam::Red {
                            middle + (3.0 + 2.0 * extra)
                        } else {
                            middle - (3.0 + 2.0 * extra)
                        };
                        let pos = Point3::new(0.5, 2.0, z);
                        spawns.push((pos, team, player_index));
                    }
                    info!("{} {}", player_name, winrate);
                }
            }
        }

        for (pos, team, player_index) in spawns {
            let rot = Rotation3::from_euler_angles(0.0, 3.0 * FRAC_PI_2, 0.0);
            server.spawn_skater(player_index, team, pos, rot);
        }

        let mut rand = rand::thread_rng();
        red_player_names.shuffle(&mut rand);
        blue_player_names.shuffle(&mut rand);
        let mut red_iter = red_player_names.into_iter().cycle();
        let mut blue_iter = blue_player_names.into_iter().cycle();
        self.rhqm_game.red_goalie = RHQMGameGoalie::TakeTurns {
            first: red_iter.next().unwrap(),
            second: red_iter.next().unwrap(),
            third: red_iter.next().unwrap(),
            overtime: red_iter.next().unwrap(),
        };
        self.rhqm_game.blue_goalie = RHQMGameGoalie::TakeTurns {
            first: blue_iter.next().unwrap(),
            second: blue_iter.next().unwrap(),
            third: blue_iter.next().unwrap(),
            overtime: blue_iter.next().unwrap(),
        };

        let msg = format!(
            "[Server] Red ({:.2}%) vs Blue ({:.2}%)",
            sum_red / 4.0,
            sum_blue / 4.0
        );

        server.messages.add_server_chat_message(msg);
    }

    pub fn send_report(
        &mut self,
        server: &mut HQMServer,
        from: HQMServerPlayerIndex,
        to: HQMServerPlayerIndex,
    ) {
        if !matches!(self.status, State::Game { .. }) {
            server
                .messages
                .add_directed_server_chat_message_str("[Server] You can report only in game", from);
        } else if to == from {
            server
                .messages
                .add_directed_server_chat_message_str("[Server] You can't report yourself", from);
        } else {
            if let Some(from_player) = self.rhqm_game.get_player_by_index(from) {
                let from_name = from_player.player_name.clone();
                let from_id = from_player.player_id;
                if let Some(to_player) = self.rhqm_game.get_player_by_index(to) {
                    let to_name = to_player.player_name.clone();
                    let to_id = to_player.player_id;
                    let pool = self.pool.clone();
                    let sender = self.sender.clone();
                    tokio::spawn(async move {
                        let client = pool.get().await?;
                        let sql1 = "SELECT count(\"Id\") FROM public.\"Reports\" WHERE \"ReportBy\" = $1 AND DATE_PART('day', NOW() - \"DateReported\")<7";
                        let res = client.query(sql1, &[&from_id]).await?;

                        let res = &res[0];
                        if res.get::<_, i64>(0) > 0 {
                            sender.send(PostgresResponse::TooManyReports { player_index: from })?;
                        } else {
                            let insert_sql = "INSERT INTO public.\"Reports\" VALUES ((select CASE WHEN max(\"Id\") IS NULL THEN 1 ELSE max(\"Id\")+1 END from public.\"Reports\"), $1, $2, NOW());";
                            client.execute(insert_sql, &[&to_id, &from_id]).await?;

                            sender.send(PostgresResponse::PlayerReported { to_name, from_name })?;
                        }
                        Ok::<_, anyhow::Error>(())
                    });
                } else if let Some(to_player) = server.players.get(to) {
                    let msg = format!("[Server] {} is not in game", to_player.player_name);
                    server.messages.add_directed_server_chat_message(msg, from);
                } else {
                    server.messages.add_directed_server_chat_message_str(
                        "[Server] Player with entered ID wasn't found",
                        from,
                    );
                }
            } else {
                server
                    .messages
                    .add_directed_server_chat_message_str("[Server] You are not in game", from);
            }
        }
    }

    pub(crate) fn fix_standins(&mut self, server: &mut HQMServer) {
        self.rhqm_game.rejoin_timer.retain(|_, v| {
            *v = v.saturating_sub(1);
            *v > 0
        });

        let mut red_count = 0;
        let mut blue_count = 0;
        let mut red_actual_players = 0;
        let mut blue_actual_players = 0;
        let mut move_to_spectators = SmallVec::<[_; 8]>::new();
        let mut red_real_player_want_to_join = SmallVec::<[_; 8]>::new();
        let mut blue_real_player_want_to_join = SmallVec::<[_; 8]>::new();
        let mut red_standin_want_to_join = SmallVec::<[_; 8]>::new();
        let mut blue_standin_want_to_join = SmallVec::<[_; 8]>::new();
        let mut red_standins = SmallVec::<[_; 8]>::new();
        let mut blue_standins = SmallVec::<[_; 8]>::new();

        for rhqm_player in self.rhqm_game.game_players.iter() {
            match rhqm_player.player_team {
                Some(HQMTeam::Red) => {
                    red_actual_players += 1;
                }
                Some(HQMTeam::Blue) => {
                    blue_actual_players += 1;
                }
                _ => {}
            };
        }
        for (player_index, player) in server.players.iter() {
            if let Some(player) = player {
                let rhqm_player = self.rhqm_game.get_player_by_index(player_index);
                if let Some((_, team)) = player.object {
                    if player.input.spectate() {
                        self.rhqm_game.rejoin_timer.insert(player_index, 500);
                        move_to_spectators.push(player_index);
                    } else {
                        match team {
                            HQMTeam::Red => {
                                red_count += 1;
                            }
                            HQMTeam::Blue => {
                                blue_count += 1;
                            }
                        }
                        if rhqm_player.is_none() {
                            match team {
                                HQMTeam::Red => {
                                    red_standins.push(player_index);
                                }
                                HQMTeam::Blue => {
                                    blue_standins.push(player_index);
                                }
                            }
                        }
                    }
                } else {
                    if !self.rhqm_game.rejoin_timer.contains_key(&player_index) {
                        if let Some(rhqm_player) = rhqm_player {
                            if let Some(team) = rhqm_player.player_team {
                                if team == HQMTeam::Red && player.input.join_red() {
                                    red_real_player_want_to_join.push(player_index);
                                } else if team == HQMTeam::Blue && player.input.join_blue() {
                                    blue_real_player_want_to_join.push(player_index);
                                }
                            }
                        } else if player.input.join_red() {
                            if self
                                .queued_players
                                .iter()
                                .any(|x| x.player_index == player_index)
                            {
                                red_standin_want_to_join.push(player_index);
                            }
                        } else if player.input.join_blue() {
                            if self
                                .queued_players
                                .iter()
                                .any(|x| x.player_index == player_index)
                            {
                                blue_standin_want_to_join.push(player_index);
                            }
                        }
                    }
                }
            }
        }
        for player_index in move_to_spectators {
            server.move_to_spectator(player_index);
        }
        for player_index in red_real_player_want_to_join {
            red_count += 1;
            server.spawn_skater_at_spawnpoint(player_index, HQMTeam::Red, HQMSpawnPoint::Bench);
        }
        for player_index in blue_real_player_want_to_join {
            blue_count += 1;
            server.spawn_skater_at_spawnpoint(player_index, HQMTeam::Blue, HQMSpawnPoint::Bench);
        }
        if red_count < red_actual_players {
            for player_index in red_standin_want_to_join
                .into_iter()
                .take(red_actual_players - red_count)
            {
                red_count += 1;
                server.spawn_skater_at_spawnpoint(player_index, HQMTeam::Red, HQMSpawnPoint::Bench);
            }
        } else if red_count > red_actual_players {
            for player_index in red_standins
                .into_iter()
                .take(red_count - red_actual_players)
            {
                red_count -= 1;
                server.move_to_spectator(player_index);
            }
        }
        if blue_count < blue_actual_players {
            for player_index in blue_standin_want_to_join
                .into_iter()
                .take(blue_actual_players - blue_count)
            {
                blue_count += 1;
                server.spawn_skater_at_spawnpoint(
                    player_index,
                    HQMTeam::Blue,
                    HQMSpawnPoint::Bench,
                );
            }
        } else if blue_count > blue_actual_players {
            for player_index in blue_standins
                .into_iter()
                .take(blue_count - blue_actual_players)
            {
                blue_count -= 1;
                server.move_to_spectator(player_index);
            }
        }
    }

    pub(crate) fn save_data(&mut self, server: &mut HQMServer) {
        let max = 30;
        let min = 15;

        let mut red_max = 0;
        let mut blue_max = 0;

        let mut red_min = 1000;
        let mut blue_min = 1000;

        for i in self.rhqm_game.game_players.iter() {
            if i.player_team == Some(HQMTeam::Red) {
                if i.player_points > red_max {
                    red_max = i.player_points;
                }
                if i.player_points < red_min {
                    red_min = i.player_points;
                }
            } else if i.player_team == Some(HQMTeam::Blue) {
                if i.player_points > blue_max {
                    blue_max = i.player_points;
                }
                if i.player_points < blue_min {
                    blue_min = i.player_points;
                }
            }
        }

        if red_min == 1000 {
            red_min = 0;
        }

        if blue_min == 1000 {
            blue_min = 0;
        }

        let red_div = red_max - red_min;
        let blue_div = blue_max - blue_min;

        let mut red_one_point: f32 = 0.0;
        if red_div != 0 {
            red_one_point = (((max - min) as f32) / red_div as f32) as f32;
        }

        let mut blue_one_point: f32 = 0.0;
        if blue_div != 0 {
            blue_one_point = (((max - min) as f32) / blue_div as f32) as f32;
        }

        let mut max_points = 0;
        let mut max_id: i32 = -1;
        let mut max_name = String::from("");

        struct PlayerStats {
            player_id: i32,
            player_team: HQMTeam,
            goals: i32,
            assists: i32,
            left: bool,
            ping: i32,
            points: i32,
            touches: i32,
            ip: String,
            position: String,
        }

        let mut player_stats = SmallVec::<[_; 16]>::new();
        for i in self.rhqm_game.game_players.iter().cloned() {
            match i {
                RHQMGamePlayer {
                    player_id,
                    player_index,
                    player_name,
                    player_points,
                    player_team,
                    goals,
                    assists,
                    left_seconds,
                    touches,
                    ..
                } => {
                    let mut points: i32;

                    let player_team = if let Some(player_team) = player_team {
                        player_team
                    } else {
                        continue;
                    };

                    if player_team == HQMTeam::Red {
                        if server.game.red_score > server.game.blue_score {
                            points = min
                                + ((red_div - (player_points - red_min)) as f32 * red_one_point)
                                    as i32;

                            if goals + assists > max_points {
                                max_points = goals + assists;
                                max_id = player_id;
                                max_name = player_name.clone();
                            }
                        } else {
                            points = -1
                                * (min + ((player_points - red_min) as f32 * red_one_point) as i32);
                        }
                    } else {
                        if server.game.red_score < server.game.blue_score {
                            points = min
                                + (((blue_div - (player_points - blue_min)) as f32)
                                    * blue_one_point) as i32;

                            if goals + assists > max_points {
                                max_points = goals + assists;
                                max_id = player_id;
                                max_name = player_name.clone();
                            }
                        } else {
                            points = -1
                                * (min
                                    + (((player_points - blue_min) as f32) * blue_one_point)
                                        as i32);
                        }
                    }

                    let left = if left_seconds == 0 {
                        points = -30;
                        true
                    } else {
                        false
                    };

                    let player = player_index.and_then(|x| server.players.get(x));

                    let ping = player
                        .and_then(|player| player.ping_data())
                        .map(|x| x.avg * 1000.0)
                        .unwrap_or(0.0);

                    let ip = player
                        .and_then(|player| player.addr())
                        .map_or_else(|| "".to_owned(), |addr| addr.ip().to_string());

                    let player_stat = PlayerStats {
                        player_id,
                        player_team,
                        goals: goals as i32,
                        assists: assists as i32,
                        left,
                        ping: ping as i32,
                        points,
                        touches: touches as i32,
                        ip,
                        position: "".to_owned(),
                    };

                    player_stats.push(player_stat);
                }
            }
        }

        let events = self.rhqm_game.events.clone();

        let xpoints = self.rhqm_game.xpoints.clone();
        let zpoints = self.rhqm_game.zpoints.clone();

        let pool = self.pool.clone();
        let red_score = server.game.red_score as i32;
        let blue_score = server.game.blue_score as i32;
        let ms = format!("[Server] Ranked game ended I MVP:{}", max_name);
        server.messages.add_server_chat_message(ms);
        tokio::spawn(async move {
            let mut client = pool.get().await?;
            let transaction = client.transaction().await?;

            let rater = Rater::new(4.16);
            let mut red_ratings = vec![];
            let mut red_ids = vec![];
            let mut blue_ratings = vec![];
            let mut blue_ids = vec![];

            let player_stat_sql = "insert into public.\"GameStats\" values((select max(\"Id\")+1 from public.\"GameStats\"), (select max(\"Id\")+1 from public.\"Stats\"), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,'','','' )";
            for player_stat in player_stats {
                let player_team_num = if player_stat.player_team == HQMTeam::Red {
                    0
                } else {
                    1
                };
                let row = transaction.query("select \"Mu\", \"Sigma\" from public.\"PlayerRatings\" where \"PlayerId\"=$1", &[&player_stat.player_id]).await?;
                let row = row.get(0);
                let rating = row
                    .map(|row| {
                        let mu = row.get(0);
                        let sigma = row.get(1);
                        Rating::new(mu, sigma)
                    })
                    .unwrap_or_else(|| Rating::default());
                if player_stat.player_team == HQMTeam::Red {
                    red_ids.push(player_stat.player_id);
                    red_ratings.push(rating);
                } else {
                    blue_ids.push(player_stat.player_id);
                    blue_ratings.push(rating);
                }
                transaction
                    .execute(
                        player_stat_sql,
                        &[
                            &player_stat.player_id,
                            &player_team_num,
                            &player_stat.goals,
                            &player_stat.assists,
                            &player_stat.points,
                            &player_stat.left,
                            &player_stat.ping,
                            &player_stat.ip,
                            &player_stat.touches,
                            &player_stat.position,
                        ],
                    )
                    .await?;
            }

            let game_event_sql = "insert into public.\"GameEvents\" values((select max(\"Id\")+1 from public.\"GameEvents\"), (select max(\"Id\")+1 from public.\"Stats\"), $1, $2, $3, $4, $5)";
            for event in events {
                let speed = event.speed as f64;
                let type_id = event.type_id as i32;
                let time = event.time as i32;
                let period = event.period as i32;
                transaction
                    .execute(
                        game_event_sql,
                        &[&event.player_id, &type_id, &speed, &time, &period],
                    )
                    .await?;
            }

            let game_score_sql = "insert into public.\"Stats\" values((select max(\"Id\")+1 from public.\"Stats\"), (select currentseason()), $1,$2,NOW(), $3,$4,$5)";

            transaction
                .execute(
                    game_score_sql,
                    &[&red_score, &blue_score, &max_id, &xpoints, &zpoints],
                )
                .await?;
            // We have to run this one last for trigger reasons

            let result = if red_score > blue_score {
                vec![1, 2]
            } else if red_score < blue_score {
                vec![2, 1]
            } else {
                vec![1, 1]
            };

            let a = rater
                .update_ratings(vec![red_ratings, blue_ratings], result)
                .unwrap();
            let mut update = vec![];
            update.extend(
                a[0].iter()
                    .zip(red_ids)
                    .map(|(rating, player_id)| (player_id, rating)),
            );
            update.extend(
                a[1].iter()
                    .zip(blue_ids)
                    .map(|(rating, player_id)| (player_id, rating)),
            );
            for (player_id, rating) in update {
                let mu = rating.mu();
                let sigma = rating.sigma();
                transaction.execute("insert into public.\"PlayerRatings\"(\"PlayerId\", \"Mu\", \"Sigma\") values($1, $2, $3) \
                ON CONFLICT (\"PlayerId\") DO UPDATE SET \"Mu\" = $2, \"Sigma\" = $3",
                                    &[&player_id, &mu, &sigma]).await?;
            }

            transaction.commit().await?;

            let game_id_url = "https://api.hqmranked.ru:5000/api/game/lastgame".to_string();

            let baseurl = "https://api.hqmranked.ru:5000/api/game/elogame".to_string();

            let client = reqwest::Client::new();
            let resmax = client.get(game_id_url).send().await?.text().await?;

            let mut map = HashMap::new();
            map.insert("game", resmax);

            let res = client.post(baseurl).json(&map).send().await;

            Ok::<_, anyhow::Error>(())
        });
    }

    fn successful_login(
        &mut self,
        server: &mut HQMServer,
        player_index: HQMServerPlayerIndex,
        player_id: i32,
        old_nickname: String,
    ) {
        if let Some(player) = server.players.get(player_index) {
            self.verified_players.insert(player_index, player_id);
            let rhqm_player = self.rhqm_game.get_player_by_id_mut(player_id);
            let is_on_ice = player.object.is_some();
            let queued = self
                .queued_players
                .iter()
                .any(|x| &x.player_id == &player_id);
            if let Some(rhqm_player) = rhqm_player {
                if rhqm_player.player_index.is_some() {
                    server.messages.add_directed_server_chat_message_str(
                        "You are already logged in",
                        player_index,
                    );
                } else {
                    rhqm_player.player_index = Some(player_index);
                    if !is_on_ice && matches!(self.status, State::Game { .. }) {
                        if let Some(team) = rhqm_player.player_team {
                            server.spawn_skater_at_spawnpoint(
                                player_index,
                                team,
                                HQMSpawnPoint::Bench,
                            );
                        }
                    }
                }
            } else if queued {
                server.messages.add_directed_server_chat_message_str(
                    "You are already logged in",
                    player_index,
                );
            } else {
                let name = player.player_name.to_string();
                let player_item = RHQMQueuePlayer {
                    player_id,
                    player_index,
                    player_name: name.clone(),
                    afk: false,
                };
                self.queued_players.push(player_item);

                let mut old_nickname_text = String::from("");

                if old_nickname.len() != 0 {
                    old_nickname_text = format!(" ({})", old_nickname);
                }

                let msg = format!(
                    "[Server] {}{} logged in [{}/{}]",
                    name,
                    old_nickname_text,
                    self.queued_players.len().to_string(),
                    self.config.team_max * 2
                );

                server.messages.add_server_chat_message(msg);
                self.rhqm_game.need_to_send = true;
            }
        }
    }

    pub(crate) fn handle_responses(&mut self, server: &mut HQMServer) {
        let responses: Vec<PostgresResponse> = self.receiver.try_iter().collect();
        for response in responses {
            match response {
                PostgresResponse::LoginFailed { player_index } => {
                    server
                        .messages
                        .add_directed_server_chat_message_str("Wrong password", player_index);
                }
                PostgresResponse::LoginSuccessful {
                    player_id,
                    player_index,
                    old_nickname,
                } => {
                    self.successful_login(server, player_index, player_id, old_nickname);
                }
                PostgresResponse::LoginBanned { player_index } => {
                    server
                        .messages
                        .add_directed_server_chat_message_str("You are banned", player_index);
                }
                PostgresResponse::TooManyReports { player_index } => {
                    server.messages.add_directed_server_chat_message_str(
                        "[Server] You can report one player each week",
                        player_index,
                    );
                }
                PostgresResponse::PlayerReported { from_name, to_name } => {
                    let s = format!("[Server] {} has been reported by {}", to_name, from_name);
                    server.messages.add_server_chat_message(s);
                }
                PostgresResponse::PlayerData { data } => {
                    if !matches!(
                        self.status,
                        State::Waiting {
                            waiting_for_response: true
                        }
                    ) {
                        continue;
                    }
                    let mut successful = true;
                    let mut new_players = vec![];
                    let mut sum = 0.0;
                    let mut player_indices = vec![];
                    for (player_id, (points, winrate, rating)) in data {
                        let queue_player = self
                            .queued_players
                            .iter()
                            .find(|q| q.player_id == player_id);
                        if let Some(queue_player) = queue_player {
                            let rhqm_player = RHQMGamePlayer {
                                player_id,
                                player_name: queue_player.player_name.clone(),
                                player_index: Some(queue_player.player_index),
                                player_points: points,
                                player_team: None,
                                goals: 0,
                                assists: 0,
                                left_seconds: 120,
                                koef: 0,
                                touches: 0,
                                winrate,
                                rating,
                            };
                            player_indices.push(queue_player.player_index);
                            new_players.push(rhqm_player);
                            info!("{} {} {}", sum, points, queue_player.player_name);
                            sum = sum + winrate;
                        } else {
                            successful = false;
                        }
                    }
                    if successful {
                        self.paused = false;
                        self.force_players_off_ice_by_system(server);
                        server.game.time = 2000;
                        self.rhqm_game.game_players = new_players;
                        self.queued_players
                            .retain(|x| !player_indices.contains(&x.player_index));
                        if self.config.picking_mode == RankedPickingMode::CaptainsPick {
                            self.status = State::CaptainsPicking {
                                time_left: 2000,
                                options: self.set_captains(server),
                                current_team: HQMTeam::Red,
                            };
                            self.send_available_picks(server);
                        } else {
                            self.status = State::Game { paused: false };
                            self.set_teams_by_server(server, sum);
                        }
                    } else {
                        self.status = State::Waiting {
                            waiting_for_response: false,
                        }
                    }
                }
                PostgresResponse::DailyEvent { text, player_index } => {
                    let daily_text = format!("Daily event: {}", text);
                    server
                        .messages
                        .add_directed_server_chat_message(daily_text, player_index);
                }
            }
        }
    }

    pub fn update_players(&mut self, server: &mut HQMServer) {
        self.handle_responses(server);

        if let State::Waiting {
            waiting_for_response,
        } = self.status
        {
            let mut players_to_spawn = smallvec::SmallVec::<[HQMServerPlayerIndex; 16]>::new();
            for queue_player in self.queued_players.iter() {
                if let Some(player) = server.players.get(queue_player.player_index) {
                    if player.object.is_none() {
                        players_to_spawn.push(queue_player.player_index);
                    }
                }
            }
            let middle = server.game.world.rink.length / 2.0;
            for (i, player_index) in players_to_spawn.into_iter().enumerate() {
                let pos = Point3::new(0.5, 2.0, middle - 10.0 + 2.0 * (i as f32));
                let rot = Rotation3::from_euler_angles(0.0, 3.0 * FRAC_PI_2, 0.0);
                server.spawn_skater(player_index, HQMTeam::Red, pos, rot);
            }
            if !waiting_for_response && self.queued_players.len() >= self.config.team_max * 2 {
                let slice = &self.queued_players[0..(self.config.team_max * 2)];
                let (players, ips, player_ids) = self.get_player_and_ips(server, slice);

                self.send_notify(players, ips, server.config.server_name.to_owned());
                self.status = State::Waiting {
                    waiting_for_response: true,
                };
                self.request_player_points_and_win_rate(player_ids);
            } else if self.rhqm_game.notify_timer % 2000 == 0 && self.rhqm_game.need_to_send {
                let (players, ips, _) = self.get_player_and_ips(server, &self.queued_players);
                self.send_notify(players, ips, server.config.server_name.to_owned());
            }
        } else if let State::Game { paused } = self.status {
            if server.game.time % 1000 == 0 && !paused {
                if let Some(puck) = server.game.world.objects.get_puck(HQMObjectIndex(0)) {
                    self.rhqm_game.xpoints.push(puck.body.pos.x);
                    self.rhqm_game.zpoints.push(puck.body.pos.z);
                }
            }

            if server.game.game_over {
                if !self.rhqm_game.data_saved {
                    self.rhqm_game.data_saved = true;
                    self.save_data(server);
                }
            }

            self.fix_standins(server);
        } else if let State::CaptainsPicking {
            ref mut time_left, ..
        } = self.status
        {
            *time_left -= 1;
            if *time_left == 0 {
                self.make_default_pick(server);
            }
        }

        self.rhqm_game.notify_timer += 1;
    }
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum HQMIcingConfiguration {
    Off,
    Touch,
    NoTouch,
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum HQMOffsideConfiguration {
    Off,
    Delayed,
    Immediate,
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum HQMTwoLinePassConfiguration {
    Off,
    On,
    Forward,
    Double,
    ThreeLine,
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum HQMOffsideLineConfiguration {
    OffensiveBlue,
    Center,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HQMPassPosition {
    None,
    ReachedOwnBlue,
    PassedOwnBlue,
    ReachedCenter,
    PassedCenter,
    ReachedOffensive,
    PassedOffensive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HQMPass {
    pub team: HQMTeam,
    pub side: HQMRinkSide,
    pub from: Option<HQMPassPosition>,
    pub player: HQMServerPlayerIndex,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum HQMIcingStatus {
    No,                            // No icing
    Warning(HQMTeam, HQMRinkSide), // Puck has reached the goal line, delayed icing
    Icing(HQMTeam),                // Icing has been called
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum HQMOffsideStatus {
    Neutral,                  // No offside
    InOffensiveZone(HQMTeam), // No offside, puck in offensive zone
    Warning(
        HQMTeam,
        HQMRinkSide,
        Option<HQMPassPosition>,
        HQMServerPlayerIndex,
    ), // Warning, puck entered offensive zone in an offside situation but not touched yet
    Offside(HQMTeam),         // Offside has been called
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum HQMTwoLinePassStatus {
    No, // No offside
    Warning(
        HQMTeam,
        HQMRinkSide,
        HQMPassPosition,
        Vec<HQMServerPlayerIndex>,
    ), // Warning, puck entered offensive zone in an offside situation but not touched yet
    Offside(HQMTeam), // Offside has been called
}

#[derive(Debug, Clone)]
pub struct HQMPuckTouch {
    pub player_index: HQMServerPlayerIndex,
    pub skater_index: HQMObjectIndex,
    pub team: HQMTeam,
    pub puck_pos: Point3<f32>,
    pub puck_speed: f32,
    pub first_time: u32,
    pub last_time: u32,
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

pub fn get_faceoff_positions(
    players: &HQMServerPlayerList,
    preferred_positions: &HashMap<HQMServerPlayerIndex, String>,
    world: &HQMGameWorld,
) -> HashMap<HQMServerPlayerIndex, (HQMTeam, String)> {
    let allowed_positions = &world.rink.allowed_positions;
    let mut res = HashMap::new();

    let mut red_players = smallvec::SmallVec::<[_; 32]>::new();
    let mut blue_players = smallvec::SmallVec::<[_; 32]>::new();
    for (player_index, player) in players.iter() {
        if let Some(player) = player {
            let team = player.object.map(|x| x.1);

            let preferred_position = preferred_positions.get(&player_index).map(String::as_str);

            if team == Some(HQMTeam::Red) {
                red_players.push((player_index, preferred_position));
            } else if team == Some(HQMTeam::Blue) {
                blue_players.push((player_index, preferred_position));
            }
        }
    }

    setup_position(&mut res, &red_players, allowed_positions, HQMTeam::Red);
    setup_position(&mut res, &blue_players, allowed_positions, HQMTeam::Blue);

    res
}

pub fn is_past_line(
    server: &HQMServer,
    player: &HQMServerPlayer,
    team: HQMTeam,
    line: &HQMRinkLine,
) -> bool {
    if let Some((object_index, skater_team)) = player.object {
        if skater_team == team {
            if let Some(skater) = server.game.world.objects.get_skater(object_index) {
                let feet_pos =
                    &skater.body.pos - (&skater.body.rot * Vector3::y().scale(skater.height));
                let dot = (&feet_pos - &line.point).dot(&line.normal);
                let leading_edge = -(line.width / 2.0);
                if dot < leading_edge {
                    // Player is past line
                    return true;
                }
            }
        }
    }
    false
}

pub fn has_players_in_offensive_zone(
    server: &HQMServer,
    team: HQMTeam,
    ignore_player: Option<HQMServerPlayerIndex>,
) -> bool {
    let line = match team {
        HQMTeam::Red => &server.game.world.rink.red_lines_and_net.offensive_line,
        HQMTeam::Blue => &server.game.world.rink.blue_lines_and_net.offensive_line,
    };

    for (player_index, player) in server.players.iter() {
        if Some(player_index) == ignore_player {
            continue;
        }
        if let Some(player) = player {
            if is_past_line(server, player, team, line) {
                return true;
            }
        }
    }

    false
}

fn setup_position(
    positions: &mut HashMap<HQMServerPlayerIndex, (HQMTeam, String)>,
    players: &[(HQMServerPlayerIndex, Option<&str>)],
    allowed_positions: &[String],
    team: HQMTeam,
) {
    let mut available_positions = Vec::from(allowed_positions);

    // First, we try to give each player its preferred position
    for (player_index, player_position) in players.iter() {
        if let Some(player_position) = player_position {
            if let Some(x) = available_positions
                .iter()
                .position(|x| x == *player_position)
            {
                let s = available_positions.remove(x);
                positions.insert(*player_index, (team, s));
            }
        }
    }

    // Some players did not get their preferred positions because they didn't have one,
    // or because it was already taken
    for (player_index, player_position) in players.iter() {
        if !positions.contains_key(player_index) {
            let s = if let Some(x) = available_positions.iter().position(|x| x == "C") {
                // Someone needs to be C
                let x = available_positions.remove(x);
                (team, x)
            } else if !available_positions.is_empty() {
                // Give out the remaining positions
                let x = available_positions.remove(0);
                (team, x)
            } else {
                // Oh no, we're out of legal starting positions
                if let Some(player_position) = player_position {
                    (team, (*player_position).to_owned())
                } else {
                    (team, "C".to_owned())
                }
            };
            positions.insert(*player_index, s);
        }
    }

    if let Some(x) = available_positions.iter().position(|x| x == "C") {
        let mut change_index = None;
        for (player_index, _) in players.iter() {
            if change_index.is_none() {
                change_index = Some(player_index);
            }

            if let Some((_, pos)) = positions.get(player_index) {
                if pos != "G" {
                    change_index = Some(player_index);
                    break;
                }
            }
        }

        if let Some(change_index) = change_index {
            let c = available_positions.remove(x);
            positions.insert(*change_index, (team, c));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::hqm_behaviour_extra::setup_position;
    use crate::hqm_game::HQMTeam;
    use crate::hqm_ranked_util::setup_position;
    use crate::hqm_server::HQMServerPlayerIndex;
    use std::collections::HashMap;

    #[test]
    fn test1() {
        let allowed_positions: Vec<String> = vec![
            "C", "LW", "RW", "LD", "RD", "G", "LM", "RM", "LLM", "RRM", "LLD", "RRD", "CM", "CD",
            "LW2", "RW2", "LLW", "RRW",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let c = "C";
        let lw = "LW";
        let rw = "RW";
        let g = "G";
        let mut res1 = HashMap::new();
        let players = vec![(HQMServerPlayerIndex(0), None)];
        setup_position(
            &mut res1,
            players.as_ref(),
            &allowed_positions,
            HQMTeam::Red,
        );
        assert_eq!(res1[&HQMServerPlayerIndex(0)].1, "C");

        let mut res1 = HashMap::new();
        let players = vec![(HQMServerPlayerIndex(0), Some(c))];
        setup_position(
            &mut res1,
            players.as_ref(),
            &allowed_positions,
            HQMTeam::Red,
        );
        assert_eq!(res1[&HQMServerPlayerIndex(0)].1, "C");

        let mut res1 = HashMap::new();
        let players = vec![(HQMServerPlayerIndex(0), Some(lw))];
        setup_position(
            &mut res1,
            players.as_ref(),
            &allowed_positions,
            HQMTeam::Red,
        );
        assert_eq!(res1[&HQMServerPlayerIndex(0)].1, "C");

        let mut res1 = HashMap::new();
        let players = vec![(HQMServerPlayerIndex(0), Some(g))];
        setup_position(
            &mut res1,
            players.as_ref(),
            &allowed_positions,
            HQMTeam::Red,
        );
        assert_eq!(res1[&HQMServerPlayerIndex(0)].1, "C");

        let mut res1 = HashMap::new();
        let players = vec![
            (HQMServerPlayerIndex(0usize), Some(c)),
            (HQMServerPlayerIndex(1), Some(lw)),
        ];
        setup_position(
            &mut res1,
            players.as_ref(),
            &allowed_positions,
            HQMTeam::Red,
        );
        assert_eq!(res1[&HQMServerPlayerIndex(0)].1, "C");
        assert_eq!(res1[&HQMServerPlayerIndex(1)].1, "LW");

        let mut res1 = HashMap::new();
        let players = vec![
            (HQMServerPlayerIndex(0), None),
            (HQMServerPlayerIndex(1), Some(lw)),
        ];
        setup_position(
            &mut res1,
            players.as_ref(),
            &allowed_positions,
            HQMTeam::Red,
        );
        assert_eq!(res1[&HQMServerPlayerIndex(0)].1, "C");
        assert_eq!(res1[&HQMServerPlayerIndex(1)].1, "LW");

        let mut res1 = HashMap::new();
        let players = vec![
            (HQMServerPlayerIndex(0), Some(rw)),
            (HQMServerPlayerIndex(1), Some(lw)),
        ];
        setup_position(
            &mut res1,
            players.as_ref(),
            &allowed_positions,
            HQMTeam::Red,
        );
        assert_eq!(res1[&HQMServerPlayerIndex(0)].1, "C");
        assert_eq!(res1[&HQMServerPlayerIndex(1)].1, "LW");

        let mut res1 = HashMap::new();
        let players = vec![
            (HQMServerPlayerIndex(0), Some(g)),
            (HQMServerPlayerIndex(1), Some(lw)),
        ];
        setup_position(
            &mut res1,
            players.as_ref(),
            &allowed_positions,
            HQMTeam::Red,
        );
        assert_eq!(res1[&HQMServerPlayerIndex(0)].1, "G");
        assert_eq!(res1[&HQMServerPlayerIndex(1)].1, "C");

        let mut res1 = HashMap::new();
        let players = vec![
            (HQMServerPlayerIndex(0usize), Some(c)),
            (HQMServerPlayerIndex(1), Some(c)),
        ];
        setup_position(
            &mut res1,
            players.as_ref(),
            &allowed_positions,
            HQMTeam::Red,
        );
        assert_eq!(res1[&HQMServerPlayerIndex(0)].1, "C");
        assert_eq!(res1[&HQMServerPlayerIndex(1)].1, "LW");
    }
}
