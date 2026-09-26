use crate::capability::Capability;
use crate::identity::{Identity, Kind};
use crate::resource::ResourceFragment;
use std::sync::RwLock;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State { Joining, Active, Draining, Left }

#[derive(Debug, Clone)]
pub struct ParticipantSnapshot {
    pub id: Identity,
    pub capability: Capability,
    pub resources: ResourceFragment,
    pub state: State,
    pub joined_at: SystemTime,
    pub updated_at: SystemTime,
}

pub struct Participant {
    id: Identity,
    capability: Capability,
    resources: ResourceFragment,
    state: RwLock<State>,
    joined_at: SystemTime,
    updated_at: RwLock<SystemTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantError { Invalid, InvalidTransition, NotActive }

impl std::fmt::Display for ParticipantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for ParticipantError {}

impl Participant {
    pub fn new(id: Identity, capability: Capability, resources: ResourceFragment, now: SystemTime) -> Result<Self, ParticipantError> {
        if id.kind != Kind::Participant || id.validate().is_err() || capability.validate().is_err()
            || resources.validate().is_err() || resources.is_empty() || now == SystemTime::UNIX_EPOCH
        { return Err(ParticipantError::Invalid); }
        Ok(Self { id, capability, resources, state: RwLock::new(State::Joining), joined_at: now, updated_at: RwLock::new(now) })
    }

    pub fn activate(&self, now: SystemTime) -> Result<(), ParticipantError> { self.transition(State::Active, now) }
    pub fn begin_drain(&self, now: SystemTime) -> Result<(), ParticipantError> { self.transition(State::Draining, now) }
    pub fn leave(&self, now: SystemTime) -> Result<(), ParticipantError> { self.transition(State::Left, now) }

    fn transition(&self, next: State, now: SystemTime) -> Result<(), ParticipantError> {
        if now == SystemTime::UNIX_EPOCH { return Err(ParticipantError::Invalid); }
        let mut state = self.state.write().expect("participant state lock poisoned");
        let valid = matches!((*state, next),
            (State::Joining, State::Active) | (State::Joining, State::Left)
            | (State::Active, State::Draining) | (State::Active, State::Left)
            | (State::Draining, State::Left));
        if !valid { return Err(ParticipantError::InvalidTransition); }
        *state = next;
        *self.updated_at.write().expect("participant timestamp lock poisoned") = now;
        Ok(())
    }

    pub fn snapshot(&self) -> ParticipantSnapshot {
        ParticipantSnapshot {
            id: self.id.clone(),
            capability: self.capability,
            resources: self.resources,
            state: *self.state.read().expect("participant state lock poisoned"),
            joined_at: self.joined_at,
            updated_at: *self.updated_at.read().expect("participant timestamp lock poisoned"),
        }
    }

    pub fn offer(&self) -> Result<ResourceFragment, ParticipantError> {
        if *self.state.read().expect("participant state lock poisoned") != State::Active {
            return Err(ParticipantError::NotActive);
        }
        Ok(self.resources)
    }
}
