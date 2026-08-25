package conformance

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type applianceLifecycleModelFixture struct {
	Actions []struct {
		Name      string   `json:"name"`
		Model     string   `json:"model"`
		Owner     string   `json:"owner"`
		Tokens    []string `json:"tokens"`
		Functions []string `json:"functions"`
	} `json:"actions"`
	NegativeConfigs []struct {
		Config    string `json:"config"`
		Model     string `json:"model"`
		Invariant string `json:"invariant"`
	} `json:"negative_configs"`
}

func TestApplianceLifecycleModelAnchors(t *testing.T) {
	fixtureData, err := os.ReadFile("testdata/appliance-lifecycle-model.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture applianceLifecycleModelFixture
	if err := json.Unmarshal(fixtureData, &fixture); err != nil {
		t.Fatal(err)
	}

	repositoryRoot := filepath.Join("..", "..")
	models := make(map[string]string)
	loadModel := func(name string) string {
		t.Helper()
		if model, ok := models[name]; ok {
			return model
		}
		data, readErr := os.ReadFile(filepath.Join(repositoryRoot, "spec", "tla", name+".tla"))
		if readErr != nil {
			t.Fatalf("model %q: %v", name, readErr)
		}
		models[name] = string(data)
		return models[name]
	}
	for _, action := range fixture.Actions {
		if !strings.Contains(loadModel(action.Model), action.Name+" ==") {
			t.Errorf("model %q action %q is missing", action.Model, action.Name)
		}
		ownerData, err := os.ReadFile(filepath.Join(repositoryRoot, action.Owner))
		if err != nil {
			t.Errorf("implementation owner %q: %v", action.Owner, err)
			continue
		}
		owner := string(ownerData)
		for _, token := range action.Tokens {
			if !strings.Contains(owner, token) {
				t.Errorf("implementation owner %q is missing behavioral anchor %q for %q", action.Owner, token, action.Name)
			}
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(repositoryRoot, action.Owner), ownerData, 0)
		if err != nil {
			t.Errorf("implementation owner %q is not valid Go: %v", action.Owner, err)
			continue
		}
		declared := make(map[string]bool)
		ast.Inspect(file, func(node ast.Node) bool {
			if declaration, ok := node.(*ast.FuncDecl); ok {
				declared[declaration.Name.Name] = true
			}
			return true
		})
		for _, function := range action.Functions {
			if !declared[function] {
				t.Errorf("implementation owner %q is missing function %q for %q", action.Owner, function, action.Name)
			}
		}
	}

	runnerData, err := os.ReadFile(filepath.Join(repositoryRoot, "tools", "tla", "run.sh"))
	if err != nil {
		t.Fatal(err)
	}
	runner := string(runnerData)
	for _, negative := range fixture.NegativeConfigs {
		configData, err := os.ReadFile(filepath.Join(repositoryRoot, "spec", "tla", negative.Config+".cfg"))
		if err != nil {
			t.Errorf("negative config %q: %v", negative.Config, err)
			continue
		}
		config := string(configData)
		if strings.Count(config, "= FALSE") != 1 {
			t.Errorf("negative config %q must remove exactly one mechanism", negative.Config)
		}
		if !strings.Contains(config, "INVARIANT "+negative.Invariant) {
			t.Errorf("negative config %q does not name invariant %q", negative.Config, negative.Invariant)
		}
		if !strings.Contains(loadModel(negative.Model), negative.Invariant+" ==") {
			t.Errorf("model %q invariant %q is missing", negative.Model, negative.Invariant)
		}
		if !strings.Contains(runner, "run_negative "+negative.Model+" "+negative.Config+" "+negative.Invariant) {
			t.Errorf("runner does not verify %q against %q", negative.Config, negative.Invariant)
		}
	}
}
