package main

import (
    "encoding/json"
    "fmt"
    "os"
    "strings"
    "time"

    "github.com/m3db/m3/src/query/models"
    "github.com/m3db/m3/src/query/parser"
    "github.com/m3db/m3/src/query/parser/promql"
    "github.com/m3db/m3/src/query/plan"

    "github.com/pborman/getopt"
)

// FunctionNode is a JSON representation of a function.
type FunctionNode struct {
	// Name is the name of the function node.
	Name string `json:"name,omitempty"`
	Op string `json:"op,omitempty"`
	// Children are any children this function node has.
	Children []FunctionNode `json:"children,omitempty"`
	// parents are any parents this node has; this is private since it is only
	// used to construct the function node map.
	parents []FunctionNode
	// node is the actual execution node.
	node parser.Node
}

// String prints the string representation of a function node.
func (n FunctionNode) String() string {
	var sb strings.Builder
	sb.WriteString(n.Name)
	if len(n.Children) > 0 {
		sb.WriteString(": ")
		sb.WriteString(fmt.Sprint(n.Children))
	}

	return sb.String()
}

type nodeMap map[string]FunctionNode

func (n nodeMap) rootNode() (FunctionNode, error) {
	var (
		found bool
		node  FunctionNode
	)

	for _, v := range n {
		if len(v.parents) == 0 {
			if found {
				return node, fmt.Errorf(
					"multiple roots found for map: %s",
					fmt.Sprint(n),
				)
			}

			found = true
			node = v
		}
	}

	if !found {
		return node, fmt.Errorf("no root found for map: %s", fmt.Sprint(n))
	}

	return node, nil
}

func constructNodeMap(nodes parser.Nodes, edges parser.Edges) (nodeMap, error) {
	nodeMap := make(map[string]FunctionNode, len(nodes))
	for _, node := range nodes {
		name := node.Op.OpType()

		nodeMap[string(node.ID)] = FunctionNode{
			Name: name,
			Op: fmt.Sprintf("%s", node.Op),
			node: node,
		}
	}

	for _, edge := range edges {
		// NB: due to how execution occurs, `parent` and `child` have the opposite
		// meaning in the `edge` context compared to the expected lexical reading,
		// so they should be swapped here.
		parent := string(edge.ChildID)
		child := string(edge.ParentID)
		parentNode, ok := nodeMap[parent]
		if !ok {
			return nil, fmt.Errorf("parent node with ID %s not found", parent)
		}

		childNode, ok := nodeMap[child]
		if !ok {
			return nil, fmt.Errorf("child node with ID %s not found", child)
		}

		parentNode.Children = append(parentNode.Children, childNode)
		childNode.parents = append(childNode.parents, parentNode)
		nodeMap[parent] = parentNode
		nodeMap[child] = childNode
	}

	return nodeMap, nil
}

func testRequestParams() models.RequestParams {
	return models.RequestParams{
		Now:              time.Now(),
		LookbackDuration: 5 * time.Minute,
		Step:             time.Second,
	}
}

func main() {
	var (
		optQuery = getopt.StringLong("query", 'q', "", "PromQL to parse")
		optPlan  = getopt.StringLong("plan", 'p', "", 
				"physical or logic will print out the execution plan of the query, by defalut prints query's ast.")
	)
	getopt.Parse()

	if *optQuery == "" {
		getopt.Usage()
		os.Exit(1)
	}

	p, err := promql.Parse(*optQuery, time.Second, models.NewTagOptions(), promql.NewParseOptions())
	if err != nil {
		fmt.Printf("Error: %s", err.Error())
		return
	}
	nodes, edges, err := p.DAG()
	if err != nil {
		fmt.Printf("cannot extract query DAG: %s", err.Error())
		return
	}
	funcMap, err := constructNodeMap(nodes, edges)
	if err != nil {
		fmt.Printf("cannot construct node map: %s", err.Error())
		return
	}
	root, err := funcMap.rootNode()
	if err != nil {
		fmt.Printf("annot fetch root node: %s", err.Error())
		return
	}

	switch *optPlan {
	case "logical":
		if lp, err := plan.NewLogicalPlan(nodes, edges); err == nil {
			fmt.Println(lp)
		} else {
			fmt.Printf("cannot generate logical plan %s", err.Error())
		}
	case "physical":
		lp, _ := plan.NewLogicalPlan(nodes, edges)
		if pp, err := plan.NewPhysicalPlan(lp, testRequestParams()); err == nil {
			fmt.Println(pp)
		} else {
			fmt.Printf("cannot generate physical plan %s", err.Error())
		}
	default:
		b, _ := json.Marshal(root)
		fmt.Println(string(b))
		return
	}
}