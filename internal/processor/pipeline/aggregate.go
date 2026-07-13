package pipeline

// ResultDispatcherStage wraps a RequestDispatcher, returning a new
// RequestDispatcher that decorates it with additional behavior.
// Leaf stages ignore the next argument.
//
// A ResultDispatcherStage is a convenience wrapper around a
// NewXXXDispatcher(next, ...) constructor so that composition is declarative.
//
// For instance, given a series of constructors:
//
//	leaf := NewLeafDispatcher(a, b, c)
//	yyy := NewYYYDispatcher(leaf, d, e, f)
//	xxx := NewXXXDispatcher(yyy, g, h)
//
// Each dispatcher exposes a named XXXDispatcherStage helper that
// captures the extra arguments, so the above simplifies to:
//
//	dispatcher := Pipe(
//	    XXXDispatcherStage(g, h),
//	    YYYDispatcherStage(d, e, f),
//	    LeafDispatcherStage(a, b, c),
//	)
//
// Note: the equivalent using explicit closures would be something along the lines of:
//
//	dispatcher := Pipe(
//	    func(next RequestDispatcher) RequestDispatcher { return NewXXXDispatcher(next, g, h) },
//	    func(next RequestDispatcher) RequestDispatcher { return NewYYYDispatcher(next, d, e, f) },
//	    func(_ RequestDispatcher) RequestDispatcher { return NewLeafDispatcher(a, b, c) },
//	)
type ResultDispatcherStage func(next RequestDispatcher) RequestDispatcher

// Pipe composes dispatcher "stages" listed in execution order
// (outermost first, leaf last). The last stage receives nil as its
// next argument; each earlier stage wraps the result of the one
// after it.
//
//	Pipe(
//	    PreDispatcherStage(),
//	    AIMDDispatcherStage(models, globalLimit, logger),
//	    DirectDispatcherStage(resolver, logger),
//	)
//
// produces: PreDispatcher( AIMDDispatcher( DirectDispatcher ) )
func Pipe(stages ...ResultDispatcherStage) RequestDispatcher {
	if len(stages) == 0 {
		return nil
	}
	d := stages[len(stages)-1](nil)
	for i := len(stages) - 2; i >= 0; i-- {
		d = stages[i](d)
	}
	return d
}
