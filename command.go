package tikv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

type Context struct {
	context.Context
	Name   string
	Args   []string
	Out    io.Writer
	Raw    bool
	Limit  int
	Client *Client
}

type Command func(ctx *Context) error

type Desc struct {
	Proc  Command
	Arity int
}

var commands = map[string]Desc{
	"set":    Desc{Proc: Set, Arity: 2},
	"get":    Desc{Proc: Get, Arity: 1},
	"delete": Desc{Proc: Delete, Arity: 1},
	"scan":   Desc{Proc: Scan, Arity: 1},
}

func Set(ctx *Context) error {
	key, val := []byte(ctx.Args[0]), []byte(ctx.Args[1])
	if ctx.Raw {
		return rawSet(ctx, key, val)
	}
	return txnSet(ctx, key, val)
}

func txnSet(ctx *Context, key, val []byte) error {
	c := ctx.Client
	txn, err := c.s.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()
	if err := txn.Set(key, val); err != nil {
		return err
	}
	if err := txn.Commit(ctx); err != nil {
		return err
	}
	return nil
}
func rawSet(ctx *Context, key, val []byte) error {
	c := ctx.Client
	return c.raw.Put(key, val)
}

func Get(ctx *Context) error {
	key := []byte(ctx.Args[0])
	if ctx.Raw {
		return rawGet(ctx, key)
	}
	return txnGet(ctx, key)
}

func txnGet(ctx *Context, key []byte) error {
	c := ctx.Client
	txn, err := c.s.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	val, err := txn.Get(key)
	if err != nil {
		return err
	}
	fmt.Fprintln(ctx.Out, val)
	return nil
}

func rawGet(ctx *Context, key []byte) error {
	c := ctx.Client
	val, err := c.raw.Get(key)
	if err != nil {
		return err
	}
	fmt.Fprintln(ctx.Out, val)
	return nil
}

func Delete(ctx *Context) error {
	key := []byte(ctx.Args[0])
	if ctx.Raw {
		return rawDelete(ctx, key)
	}
	return txnDelete(ctx, key)
}

func txnDelete(ctx *Context, key []byte) error {
	c := ctx.Client
	txn, err := c.s.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()
	if err := txn.Delete(key); err != nil {
		return err
	}
	return txn.Commit(ctx)
}

func rawDelete(ctx *Context, key []byte) error {
	c := ctx.Client
	return c.raw.Delete(key)
}

func Scan(ctx *Context) error {
	key := []byte(ctx.Args[0])
	if ctx.Raw {
		return rawScan(ctx, key)
	}
	return txnScan(ctx, key)
}

func txnScan(ctx *Context, key []byte) error {
	c := ctx.Client

	txn, err := c.s.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	iter, err := txn.Iter(key, nil)
	if err != nil {
		return err
	}
	for iter.Valid() {
		fmt.Fprintln(ctx.Out, iter.Key(), iter.Value())
		if err := iter.Next(); err != nil {
			return err
		}
	}
	return nil
}

func rawScan(ctx *Context, key []byte) error {
	c := ctx.Client

	keys, vals, err := c.raw.Scan(key, ctx.Limit)
	if err != nil {
		return err
	}
	for i := range keys {
		fmt.Fprintln(ctx.Out, keys[i], vals[i])
	}
	return nil
}

func Call(c *Client, args []string) error {
	name, args := args[0], args[1:]
	cmd, ok := commands[name]
	if !ok {
		errors.New("unkown command")
	}

	if len(args) < cmd.Arity {
		return errors.New("wrong number of args")
	}
	out := ioutil.Discard
	if c.opt.verbose {
		out = os.Stdout
	}
	ctx := &Context{
		Name:    name,
		Args:    args,
		Out:     out,
		Raw:     c.opt.raw,
		Limit:   c.opt.limit,
		Client:  c,
		Context: context.Background(),
	}
	return cmd.Proc(ctx)
}
