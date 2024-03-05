package stage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadReassignments(t *testing.T) {
	expected := &Reassignments{
		Partitions: []Reassignment{
			{
				Topic:     "foo",
				Partition: 1,
				Replicas:  []int{1, 2, 3},
				LogDirs:   []string{"dir1", "dir2", "dir3"},
			},
		},
		Version: 1,
	}

	actual, err := readReassignments("test_reassignments.json")
	require.NoError(t, err)

	require.Equal(t, expected, actual)
}

func TestGetReassignmentSteps(t *testing.T) {
	type testCase struct {
		r                 Reassignment
		state             map[string][]int
		maxMovesPerBroker int
		expected          []*Reassignment
	}
	testCases := map[string]testCase{
		"null reassignment": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 1, 2},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2},
			},
			maxMovesPerBroker: 1,
			expected:          []*Reassignment{},
		},
		"maxMovesPerBroker=1, full replacement": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{3, 4, 5},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2},
				"foobar-1": {0, 1, 2},
			},
			maxMovesPerBroker: 1,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{3, 1, 2},
					adding:    []int{3},
					removing:  []int{0},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{3, 4, 2},
					adding:    []int{4},
					removing:  []int{1},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{3, 4, 5},
					adding:    []int{5},
					removing:  []int{2},
				},
			},
		},
		"maxMovesPerBroker=1, partial replacement": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 4, 5},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2},
			},
			maxMovesPerBroker: 1,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 4, 2},
					adding:    []int{4},
					removing:  []int{1},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 4, 5},
					adding:    []int{5},
					removing:  []int{2},
				},
			},
		},
		"maxMovesPerBroker=1, decrease rf": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 1},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2, 3},
			},
			maxMovesPerBroker: 1,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1, 3},
					adding:    nil,
					removing:  []int{2},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1},
					adding:    nil,
					removing:  []int{3},
				},
			},
		},
		"maxMovesPerBroker=1, increase rf": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 1, 2, 3},
			},
			state: map[string][]int{
				"foobar-0": {0, 1},
			},
			maxMovesPerBroker: 1,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1, 2},
					adding:    []int{2},
					removing:  nil,
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1, 2, 3},
					adding:    []int{3},
					removing:  nil,
				},
			},
		},
		"maxMovesPerBroker=2": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{3, 4, 5},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2},
			},
			maxMovesPerBroker: 2,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{3, 4, 2},
					adding:    []int{3, 4},
					removing:  []int{0, 1},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{3, 4, 5},
					adding:    []int{5},
					removing:  []int{2},
				},
			},
		},
		"maxMovesPerBroker=2, partial replacement": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 4, 5},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2},
			},
			maxMovesPerBroker: 2,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 4, 5},
					adding:    []int{4, 5},
					removing:  []int{1, 2},
				},
			},
		},
		"maxMovesPerBroker=2, decrease rf": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 1},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2, 3},
			},
			maxMovesPerBroker: 2,
			expected: []*Reassignment{

				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1},
					adding:    nil,
					removing:  []int{2, 3},
				},
			},
		},
		"maxMovesPerBroker=2, increase rf": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 1, 2, 3},
			},
			state: map[string][]int{
				"foobar-0": {0, 1},
			},
			maxMovesPerBroker: 2,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1, 2, 3},
					adding:    []int{2, 3},
					removing:  nil,
				},
			},
		},
		"maxMovesPerBroker=3": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{3, 4, 5},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2},
			},
			maxMovesPerBroker: 3,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{3, 4, 5},
					adding:    []int{3, 4, 5},
					removing:  []int{0, 1, 2},
				},
			},
		},
		"maxMovesPerBroker=3, partial replacement": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 4, 5},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2},
			},
			maxMovesPerBroker: 3,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 4, 5},
					adding:    []int{4, 5},
					removing:  []int{1, 2},
				},
			},
		},
		"maxMovesPerBroker=3, decrease rf": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 1},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2, 3},
			},
			maxMovesPerBroker: 3,
			expected: []*Reassignment{

				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1},
					adding:    nil,
					removing:  []int{2, 3},
				},
			},
		},
		"maxMovesPerBroker=3, increase rf": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 1, 2, 3},
			},
			state: map[string][]int{
				"foobar-0": {0, 1},
			},
			maxMovesPerBroker: 3,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1, 2, 3},
					adding:    []int{2, 3},
					removing:  nil,
				},
			},
		},
		"no limit": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{3, 4, 5},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2},
			},
			maxMovesPerBroker: 0,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{3, 4, 5},
					adding:    []int{3, 4, 5},
					removing:  []int{0, 1, 2},
				},
			},
		},
		"change replica set order": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 1},
			},
			state: map[string][]int{
				"foobar-0": {1, 0},
			},
			maxMovesPerBroker: 1,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1},
					swapping:  []int{0, 1},
				},
			},
		},
		"change replica set order, change replicas": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{2, 3, 4},
			},
			state: map[string][]int{
				"foobar-0": {0, 2, 1},
			},
			maxMovesPerBroker: 1,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{2, 0, 1},
					swapping:  []int{2, 0, 1},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{2, 3, 1},
					adding:    []int{3},
					removing:  []int{0},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{2, 3, 4},
					adding:    []int{4},
					removing:  []int{1},
				},
			},
		},
		"change replica set order, decrease rf": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{3, 0},
			},
			state: map[string][]int{
				"foobar-0": {0, 1, 2, 3},
			},
			maxMovesPerBroker: 1,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{3, 0, 1, 2},
					swapping:  []int{3, 0, 1, 2},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{3, 0, 2},
					removing:  []int{1},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{3, 0},
					removing:  []int{2},
				},
			},
		},
		"change replica set order, increase rf": {
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 1, 2, 3, 4},
			},
			state: map[string][]int{
				"foobar-0": {3, 0},
			},
			maxMovesPerBroker: 1,
			expected: []*Reassignment{
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 3},
					swapping:  []int{0, 3},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1, 3},
					adding:    []int{1},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1, 2, 3},
					adding:    []int{2},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1, 2, 3},
					adding:    []int{3},
				},
				{
					Topic:     "foobar",
					Partition: 0,
					Replicas:  []int{0, 1, 2, 3, 4},
					adding:    []int{4},
				},
			},
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			actual := tc.r.getReassignmentSteps(tc.state, tc.maxMovesPerBroker)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestMaybeApplyReassignment(t *testing.T) {
	type testCase struct {
		s                   state
		r                   Reassignment
		maxMovesPerBroker   int
		expected            bool
		expectedBrokers     map[int]int
		expectedAssignments map[string][]int
	}
	testCases := map[string]testCase{
		"all brokers involved, happy": {
			s: state{
				brokers: map[int]int{
					0: 0,
					1: 0,
					2: 0,
				},
				assignments: map[string][]int{
					"foobar-0": {0, 1},
				},
			},
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 2},
				adding:    []int{2},
				removing:  []int{1},
			},
			maxMovesPerBroker: 1,
			expected:          true,
			expectedBrokers: map[int]int{
				0: 1, // Lead replica.
				1: 1, // Source broker.
				2: 1, // Destination broker.
			},
			expectedAssignments: map[string][]int{
				"foobar-0": {0, 2},
			},
		},
		"other scheduled movements, not happy": {
			s: state{
				// Brokers 0 and 1 are involved in another replica movement
				// already.
				brokers: map[int]int{
					0: 1,
					1: 1,
					2: 0,
				},
				assignments: map[string][]int{
					"foobar-0": {0, 1},
				},
			},
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 2},
				adding:    []int{2},
				removing:  []int{1},
			},
			maxMovesPerBroker: 1,
			expected:          false,
			expectedBrokers: map[int]int{
				0: 1,
				1: 1,
				2: 0,
			},
			expectedAssignments: map[string][]int{
				"foobar-0": {0, 1},
			},
		},
		"other scheduled movements but within capacity, happy": {
			s: state{
				// Brokers 0 and 1 are involved in another replica movement
				// already.
				brokers: map[int]int{
					0: 1,
					1: 1,
					2: 0,
				},
				assignments: map[string][]int{
					"foobar-0": {0, 1},
				},
			},
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 2},
				adding:    []int{2},
				removing:  []int{1},
			},
			maxMovesPerBroker: 2,
			expected:          true,
			expectedBrokers: map[int]int{
				0: 2,
				1: 2,
				2: 1,
			},
			expectedAssignments: map[string][]int{
				"foobar-0": {0, 2},
			},
		},
		"larger scheduled movements but within capacity, happy": {
			s: state{
				// Brokers 0 and 1 are involved in another replica movement
				// already.
				brokers: map[int]int{
					0: 1,
					1: 1,
					2: 0,
					3: 0,
				},
				assignments: map[string][]int{
					"foobar-0": {0, 1},
				},
			},
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{2, 3},
				adding:    []int{2, 3},
				removing:  []int{0, 1},
			},
			maxMovesPerBroker: 2,
			expected:          true,
			expectedBrokers: map[int]int{
				0: 2,
				1: 2,
				2: 1,
				3: 1,
			},
			expectedAssignments: map[string][]int{
				"foobar-0": {2, 3},
			},
		},
		"larger scheduled movements, unhappy": {
			s: state{
				// Brokers 0 and 1 are involved in another replica movement
				// already.
				brokers: map[int]int{
					0: 1,
					1: 1,
					2: 2,
					3: 0,
				},
				assignments: map[string][]int{
					"foobar-0": {0, 1},
				},
			},
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{2, 3},
				adding:    []int{2, 3},
				removing:  []int{0, 1},
			},
			maxMovesPerBroker: 2,
			expected:          false,
			expectedBrokers: map[int]int{
				0: 1,
				1: 1,
				2: 2,
				3: 0,
			},
			expectedAssignments: map[string][]int{
				"foobar-0": {0, 1},
			},
		},
		"other brokers busy": {
			s: state{
				// Brokers 0 and 1 are involved in another replica movement
				// already.
				brokers: map[int]int{
					0: 1,
					1: 1,
					2: 0,
					3: 0,
					4: 0,
				},
				assignments: map[string][]int{
					"foobar-0": {2, 3},
				},
			},
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{2, 4},
				adding:    []int{4},
				removing:  []int{3},
			},
			maxMovesPerBroker: 1,
			expected:          true,
			expectedBrokers: map[int]int{
				0: 1,
				1: 1,
				2: 1,
				3: 1,
				4: 1,
			},
			expectedAssignments: map[string][]int{
				"foobar-0": {2, 4},
			},
		},
		"adding a broker": {
			s: state{
				// Brokers 0 and 1 are involved in another replica movement
				// already.
				brokers: map[int]int{
					0: 0,
					1: 0,
					2: 0,
				},
				assignments: map[string][]int{
					"foobar-0": {0},
				},
			},
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0, 1},
				adding:    []int{1},
				removing:  []int{},
			},
			maxMovesPerBroker: 1,
			expected:          true,
			expectedBrokers: map[int]int{
				0: 1,
				1: 1,
				2: 0,
			},
			expectedAssignments: map[string][]int{
				"foobar-0": {0, 1},
			},
		},
		"removing a broker": {
			s: state{
				// Brokers 0 and 1 are involved in another replica movement
				// already.
				brokers: map[int]int{
					0: 0,
					1: 0,
					2: 0,
				},
				assignments: map[string][]int{
					"foobar-0": {0, 1},
				},
			},
			r: Reassignment{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{0},
				adding:    []int{},
				removing:  []int{1},
			},
			maxMovesPerBroker: 1,
			expected:          true,
			expectedBrokers: map[int]int{
				0: 1,
				1: 1,
				2: 0,
			},
			expectedAssignments: map[string][]int{
				"foobar-0": {0},
			},
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			res := tc.s.maybeApplyReassignment(tc.r, tc.maxMovesPerBroker)

			require.Equal(t, tc.expected, res)
			require.Equal(t, tc.expectedBrokers, tc.s.brokers)
			require.Equal(t, tc.expectedAssignments, tc.s.assignments)
		})
	}
}
