package middleware

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Serializable interface {
	Serialize() ([]byte, error)
}

//=================================================================================================
// Player
//-------------------------------------------------------------------------------------------------
type PlayerRecord struct {
	Token  string  `json:"token"`
	Match  string  `json:"match"`
	Rating float32 `json:"rating"`
	Civ    string  `json:"civ"`
	Team   int     `json:"team"`
	Winner bool    `json:"winner"`
	// color  string
}

type PlayerRecordBatch struct {
	Records []*PlayerRecord `json:"records"`
}

func CreatePlayerRecordFromSlice(slice []string) *PlayerRecord {
	// Parse fields that need to be parsed.
	rating, _ := strconv.ParseFloat(slice[2], 32)
	team, _ := strconv.Atoi(slice[5])
	winner, _ := strconv.ParseBool(slice[6])

	return CreatePlayerRecord(
		slice[0],        // token
		slice[1],        // match token
		float32(rating), // rating
		slice[3],        // color
		slice[4],        // civ
		team,            // team
		winner,          // winner
	)
}

func CreatePlayerRecord(token string, match string, rating float32, color string,
	civ string, team int, winner bool) *PlayerRecord {
	// Instantiate a new player record from the data.
	record := &PlayerRecord{}
	record.Token = token
	record.Match = match
	record.Rating = rating
	// record.color = color
	record.Civ = civ
	record.Team = team
	record.Winner = winner
	return record
}

func (record *PlayerRecord) Serialize() ([]byte, error) {
	if serialized, err := json.Marshal(record); err != nil {
		return nil, err
	} else {
		return serialized, nil
	}
}

func DeserializePlayerRecord(serialized []byte) (*PlayerRecord, error) {
	var record PlayerRecord
	if err := json.Unmarshal(serialized, &record); err != nil {
		return nil, err
	} else {
		return &record, nil
	}
}

func CreatePlayerRecordBatch(records []*PlayerRecord) *PlayerRecordBatch {
	return &PlayerRecordBatch{records}
}

func (batch *PlayerRecordBatch) Serialize() ([]byte, error) {
	if serialized, err := json.Marshal(batch); err != nil {
		return nil, err
	} else {
		return serialized, nil
	}
}

func DeserializePlayerRecords(serialized []byte) (*PlayerRecordBatch, error) {
	var batch PlayerRecordBatch
	if err := json.Unmarshal(serialized, &batch); err != nil {
		return nil, err
	} else {
		return &batch, nil
	}
}

//=================================================================================================
// Match
//-------------------------------------------------------------------------------------------------
var matchDurationRegex *regexp.Regexp = regexp.MustCompile(`^([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})$`)

type MatchRecord struct {
	Token         string  `json:"token"`
	WinningTeam   int     `json:"winning_team"`
	Mirror        bool    `json:"mirror"`
	Ladder        string  `json:"ladder"`
	AverageRating float32 `json:"average_rating"`
	GameMap       string  `json:"game_map"`
	NumPlayers    int     `json:"num_players"`
	Server        string  `json:"server"`
	Duration      string  `json:"duration"`
	// patch         string
	// mapSize       string
}

type MatchRecordBatch struct {
	Records []*MatchRecord `json:"records"`
}

func CreateMatchRecordFromSlice(slice []string) *MatchRecord {
	// Parse fields that need to be parsed.
	winningTeam, _ := strconv.Atoi(slice[1])
	mirror, _ := strconv.ParseBool(slice[2])
	averageRating, _ := strconv.ParseFloat(slice[5], 32)
	numPlayers, _ := strconv.Atoi(slice[8])

	return CreateMatchRecord(
		slice[0],               // token
		winningTeam,            // winning team
		mirror,                 // mirror
		slice[3],               // ladder
		slice[4],               // patch
		float32(averageRating), // average rating
		slice[6],               // game map
		slice[7],               // map size
		numPlayers,             // num players
		slice[9],               // server
		slice[10],              // duration
	)
}

func CreateMatchRecord(token string, winningTeam int, mirror bool, ladder string, patch string,
	averageRating float32, gameMap string, mapSize string, numPlayers int, server string, duration string) *MatchRecord {
	// Instantiate and reteurn match record.
	record := &MatchRecord{}
	record.Token = token
	record.WinningTeam = winningTeam
	record.Mirror = mirror
	record.Ladder = ladder
	record.AverageRating = averageRating
	record.GameMap = gameMap
	record.NumPlayers = numPlayers
	record.Server = server
	record.Duration = duration
	// record.patch = patch
	// record.mapSize = mapSize
	return record
}

func (record *MatchRecord) InServer(server string) bool {
	return record.Server == server
}

func (record *MatchRecord) AverageRatingAbove(rating float32) bool {
	return record.AverageRating > rating
}

// A long match is defined to be longer than two hours.
func (record *MatchRecord) IsLongMatch() bool {
	// If the match lasted days, then it is definitely a long match.
	if strings.Contains(record.Duration, "day") {
		return true
	}
	// Parse the duration of the match.
	d := matchDurationRegex.FindStringSubmatch(record.Duration)
	// Return whether the match lasted longer than stated.
	matchDuration, _ := time.ParseDuration(fmt.Sprintf("%sh%sm%ss", d[1], d[2], d[3]))
	return matchDuration > time.Duration(2)*time.Hour
}

func CreateMatchRecordBatch(records []*MatchRecord) *MatchRecordBatch {
	return &MatchRecordBatch{records}
}

func (record *MatchRecord) Serialize() ([]byte, error) {
	if serialized, err := json.Marshal(record); err != nil {
		return nil, err
	} else {
		return serialized, nil
	}
}

func DeserializeMatchRecord(serialized []byte) (*MatchRecord, error) {
	var record MatchRecord
	if err := json.Unmarshal(serialized, &record); err != nil {
		return nil, err
	} else {
		return &record, nil
	}
}

func (batch *MatchRecordBatch) Serialize() ([]byte, error) {
	if serialized, err := json.Marshal(batch); err != nil {
		return nil, err
	} else {
		return serialized, nil
	}
}

func DeserializeMatchRecords(serialized []byte) (*MatchRecordBatch, error) {
	var batch MatchRecordBatch
	if err := json.Unmarshal(serialized, &batch); err != nil {
		return nil, err
	} else {
		return &batch, nil
	}
}

//=================================================================================================
// Joint Record
//-------------------------------------------------------------------------------------------------
type JointMatchRecord struct {
	MatchToken string               `json:"match_token"`
	Players    []*JointPlayerRecord `json:"match_players"`
}

type JointPlayerRecord struct {
	Token string `json:"player_token"`
}

type JointMatchRecordBatch struct {
	Records []*JointMatchRecord `json:"records"`
}

func Join(match *MatchRecord, players []*PlayerRecord) *JointMatchRecord {
	// Instantiate a joint match record from the match itself.
	record := &JointMatchRecord{}
	record.MatchToken = match.Token
	record.Players = make([]*JointPlayerRecord, len(players))

	// Set all players.
	for i, player := range players {
		record.Players[i] = &JointPlayerRecord{}
		record.Players[i].Token = player.Token
	}

	return record
}

func (record *JointMatchRecord) Serialize() ([]byte, error) {
	if serialized, err := json.Marshal(record); err != nil {
		return nil, err
	} else {
		return serialized, nil
	}
}

func DeserializeJointMatchRecord(serialized []byte) (*JointMatchRecord, error) {
	var record JointMatchRecord
	if err := json.Unmarshal(serialized, &record); err != nil {
		return nil, err
	} else {
		return &record, nil
	}
}

func (record *JointMatchRecordBatch) Serialize() ([]byte, error) {
	if serialized, err := json.Marshal(record); err != nil {
		return nil, err
	} else {
		return serialized, nil
	}
}

func DeserializeJointMatchRecords(serialized []byte) (*JointMatchRecordBatch, error) {
	var record JointMatchRecordBatch
	if err := json.Unmarshal(serialized, &record); err != nil {
		return nil, err
	} else {
		return &record, nil
	}
}

//=================================================================================================
// Results
//-------------------------------------------------------------------------------------------------

// A type of record used to notify about matches or players meeting certain criteria.
// What the token means depends on context (e.g. which queue it was sent through).
type SingleTokenRecord struct {
	Token string `json:"token"`
}

func CreateSingleTokenRecord(token string) *SingleTokenRecord {
	return &SingleTokenRecord{token}
}

func (record *SingleTokenRecord) Serialize() ([]byte, error) {
	return json.Marshal(record)
}

func DeserializeSingleTokenRecord(data []byte) (*SingleTokenRecord, error) {
	var record SingleTokenRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	} else {
		return &record, nil
	}
}
